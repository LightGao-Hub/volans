package com.haizhi.volans.sink.component

import java.util

import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.sink.config.constant.{CoreConstants, Keys, OperationMode, StoreType}
import com.haizhi.volans.sink.config.schema.SchemaVo
import com.haizhi.volans.sink.config.store.StoreEsConfig
import com.haizhi.volans.sink.server.EsDao
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

/**
 * Author pengxb
 * Date 2020/11/23
 */
class RestEsSink(override var storeType: StoreType,
                 var storeConfig: StoreEsConfig,
                 var schemaVo: SchemaVo)
  extends RichSinkFunction[Iterable[String]] with Sink {

  private val logger = LoggerFactory.getLogger(classOf[RestEsSink])
  override var uid: String = "ES"
  private var esDao: EsDao = _
  private var operationMode: OperationMode = _

  override def open(parameters: Configuration): Unit = {
    esDao = new EsDao()
    // 初始化Rest客户端
    esDao.initClient(storeConfig)
    // 创建index、type
    esDao.createTableIfNecessary(storeConfig)
    this.operationMode = OperationMode.findStoreType(schemaVo.operation.mode)
  }

  override def invoke(elements: Iterable[String], context: SinkFunction.Context[_]): Unit = {
    var deleteList: List[String] = null
    var upsertList: List[java.util.Map[String, Object]] = null

    operationMode match {
      case OperationMode.MIX =>
        // filter elements
        val filteredTuple: List[(String, java.util.Map[String, Object], Boolean)] =
          elements.map(record => {
            val recordMap = JSONUtils.jsonToJavaMap(record)
            validateAndMerge(recordMap)
            val filterFlag = recordMap.get(schemaVo.operation) != null && CoreConstants.OPERATION_DELETE.equalsIgnoreCase(recordMap.get(schemaVo.operation).toString)
            recordMap.remove(schemaVo.operation)
            // (source string,converted map,filter flag)
            (JSONUtils.toJson(recordMap), recordMap, filterFlag)
          }
          ).toList
        deleteList = filteredTuple.filter(_._3).map(_._2.get(Keys.OBJECT_KEY).toString)
        upsertList = filteredTuple.filter(!_._3).map(_._2)
      case OperationMode.DELETE =>
        deleteList = elements.map(record => {
          val recordMap = JSONUtils.jsonToJavaMap(record)
          validateAndMerge(recordMap)
          recordMap.get(Keys.OBJECT_KEY).toString
        }).toList
      case OperationMode.UPSERT =>
        upsertList = elements.map(record => {
          val recordMap = JSONUtils.jsonToJavaMap(record)
          validateAndMerge(recordMap)
          recordMap
        }).toList
    }
    // Delete List
    if (deleteList != null && deleteList.size > 0) {
      logger.debug(s"es delete list: $deleteList")
      esDao.bulkDelete(deleteList, storeConfig)
    }
    // Upsert List
    if (upsertList != null && upsertList.size > 0) {
      logger.debug(s"es upsert list: $upsertList")
      esDao.bulkUpsert(upsertList, storeConfig)
    }
  }

  /**
   * 参数校验、转换
   *
   * @param element
   */
  def validateAndMerge(element: util.Map[String, Object]): Unit = {
    if (element.containsKey(Keys.OBJECT_KEY) && !element.containsKey(Keys.ID)) {
      element.put(Keys.ID, element.get(Keys.OBJECT_KEY))
    }
  }

  override def build[T](v: T): RichSinkFunction[T] = {
    this.asInstanceOf[RichSinkFunction[T]]
  }

  override def close(): Unit = {
    // 释放资源
    esDao.shutdown()
  }
}
