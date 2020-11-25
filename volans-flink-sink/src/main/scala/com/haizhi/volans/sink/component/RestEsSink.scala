package com.haizhi.volans.sink.component

import java.util
import java.util.Properties

import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.sink.config.constant.{CoreConstants, Keys, StoreType}
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
                 var storeConfig: StoreEsConfig)
  extends RichSinkFunction[Iterable[String]] with Sink {

  private val logger = LoggerFactory.getLogger(classOf[RestEsSink])
  override var uid: String = "ES"
  private var esDao: EsDao = _

  override def open(parameters: Configuration): Unit = {
    esDao = new EsDao()
    // 初始化Rest客户端
    esDao.initClient(storeConfig)
    // 创建index、type
    esDao.createTableIfNecessary(storeConfig)
  }

  override def invoke(elements: Iterable[String], context: SinkFunction.Context[_]): Unit = {
    // filter elements
    val filteredTuple: List[(String, java.util.Map[String, Object], Boolean)] =
      elements.map(record => {
        val recordMap = JSONUtils.jsonToJavaMap(record)
        validateAndMerge(recordMap)
        val filterFlag = recordMap.get(Keys._OPERATION) != null && CoreConstants.OPERATION_DELETE.equalsIgnoreCase(recordMap.get(Keys._OPERATION).toString)
        recordMap.remove(Keys._OPERATION)
        // (source string,converted map,filter flag)
        (JSONUtils.toJson(recordMap), recordMap, filterFlag)
      }
      ).toList

    // Delete List
    val deleteList = filteredTuple.filter(_._3).map(_._2.get(Keys.OBJECT_KEY).toString)
    if(deleteList.size > 0){
      logger.debug(s"es delete list: $deleteList")
      esDao.bulkDelete(deleteList,storeConfig)
    }

    // Upsert List
    val upsertList = filteredTuple.filter(!_._3).map(_._2)
    if(upsertList.size > 0){
      logger.debug(s"es upsert list: $upsertList")
      esDao.bulkUpsert(upsertList,storeConfig)
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
