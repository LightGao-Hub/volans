package com.flink.sink.sinks

import java.util
import java.util.Properties

import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.sink.config.constant.{CoreConstants, FieldType, JavaFieldType, Keys, StoreType}
import com.haizhi.volans.sink.config.key.RowKeyGetter
import com.haizhi.volans.sink.config.schema.SchemaVo
import com.haizhi.volans.sink.server.HBaseDao
import com.haizhi.volans.sink.component.Sink
import com.haizhi.volans.sink.config.store.StoreHBaseConfig
import com.haizhi.volans.sink.utils.HbaseSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * Author pengxb
 * Date 2020/11/16
 */
class HbaseSink(override var storeType: StoreType,
                var storeConfig: StoreHBaseConfig,
                var schemaVo: SchemaVo)
  extends RichSinkFunction[Iterable[String]] with Sink {

  private val logger = LoggerFactory.getLogger(classOf[HbaseSink])
  override var uid: String = "Hbase"
  private val hbaseDao: HBaseDao = new HBaseDao()
  private var fieldTypeMap: Map[String, JavaFieldType] = _


  override def open(parameters: Configuration): Unit = {
    hbaseDao.init(storeConfig)
    hbaseDao.createTableIfNecessary(storeConfig)
    // schema字段类型转换
    fieldTypeMap = schemaVo.getScalaFields.map(elem => {
      (elem._1, FieldType.getJavaFieldType(elem._2.`type`))
    })
  }

  override def invoke(elements: Iterable[String], context: SinkFunction.Context[_]): Unit = {
    val table = hbaseDao.getTable(storeConfig.table)

    val filteredTuple = elements.map(record => {
      val recordMap = JSONUtils.jsonToJavaMap(record)
      validateAndMerge(recordMap)
      val filterFlag = recordMap.get(Keys._OPERATION) != null && CoreConstants.OPERATION_DELETE.equalsIgnoreCase(recordMap.get(Keys._OPERATION).toString)
      (recordMap, filterFlag)
    }
    )

    // Delete Operation
    val deleteList = filteredTuple
      .filter(_._2)
      .map(element => {
        new Delete(Bytes.toBytes(element._1.get(Keys._ROW_KEY).toString))
      }).toList

    // Upsert Operation
    val upsertList = filteredTuple
      .filter(!_._2)
      .map(element => {
        val recordMap = element._1
        if (recordMap.get(Keys._OPERATION) != null) {
          recordMap.remove(Keys._OPERATION)
        }
        val put = new Put(Bytes.toBytes(recordMap.get(Keys._ROW_KEY).toString))
        val iter = recordMap.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val column = entry.getKey
          if (!Keys._ROW_KEY.equals(entry.getKey)) {
            val fieldType: JavaFieldType = fieldTypeMap.getOrElse(column, JavaFieldType.UNKNOWN)
            put.addColumn(Bytes.toBytes(Keys.FAMILY), Bytes.toBytes(column), HbaseSerializer.serialize(entry.getValue, fieldType))
          }
        }
        put
      }).toList

    if (deleteList.size > 0) {
      val deletePuts: util.List[Delete] = new util.ArrayList[Delete](deleteList.size)
      deleteList.foreach(deletePuts.add(_))
      hbaseDao.bulkDelete(deletePuts, table, storeConfig.importBatchSize)
    }
    if (upsertList.size > 0) {
      hbaseDao.bulkUpsert(upsertList.asJava, table, storeConfig.importBatchSize)
    }
    hbaseDao.close(table)
  }

  def validateAndMerge(element: util.Map[String, Object]): Unit = {
    if (element.containsKey(Keys.OBJECT_KEY)) {
      element.put(Keys._ROW_KEY, RowKeyGetter.getRowKey(element.get(Keys.OBJECT_KEY).toString))
    }
  }

  override def build[T](v: T): RichSinkFunction[T] = {
    this.asInstanceOf[RichSinkFunction[T]]
  }

  override def close(): Unit = {
    hbaseDao.shutdown()
  }

}
