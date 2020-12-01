package com.haizhi.volans.sink.component

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.sink.config.constant.{CoreConstants, StoreType}
import com.haizhi.volans.sink.config.schema.SchemaVo
import com.haizhi.volans.sink.config.store._
import org.apache.commons.collections.MapUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Author pengxb
 * Date 2020/11/17
 */
object SinkContext {

  private val logger = LoggerFactory.getLogger(getClass)

  private var sinkList: List[Sink] = _
  private var schemaVo: SchemaVo = _

  /**
   * 解析上游参数
   *
   * @param configStr
   */
  def parseArgs(configStr: String): Unit = {
    val jsonMap = JSONUtils.jsonToMap(configStr)
    this.schemaVo = JSONUtils.fromJson(JSONUtils.toJson(jsonMap.get(CoreConstants.SCHEMA)), new TypeToken[SchemaVo]() {}.getType)
    this.sinkList = buildSinks(jsonMap.get(CoreConstants.SINKS))
  }

  /**
   * 构建SINK
   *
   * @param sinkCfg
   * @return
   */
  def buildSinks(sinkCfg: AnyRef): List[Sink] = {
    val sinks: mutable.Set[Sink] = new mutable.HashSet[Sink]()
    val sinkList: java.util.List[java.util.Map[String, AnyRef]] =
      JSONUtils.fromJson(JSONUtils.toJson(sinkCfg), new TypeToken[java.util.List[java.util.Map[String, AnyRef]]]() {}.getType)
    for (i <- 0 until sinkList.size()) {
      val map = sinkList.get(i)
      val storeType: StoreType = StoreType.findStoreType(MapUtils.getString(map, CoreConstants.STORE_TYPE))
      var jsonType: Type = null
      if (storeType == StoreType.ATLAS || storeType == StoreType.GDB) {
        jsonType = new TypeToken[StoreAtlasConfig]() {}.getType
      } else if (storeType == StoreType.ES) {
        jsonType = new TypeToken[StoreEsConfig]() {}.getType
      } else if (storeType == StoreType.HBASE) {
        jsonType = new TypeToken[StoreHBaseConfig]() {}.getType
      } else if (storeType == StoreType.HIVE) {
        jsonType = new TypeToken[StoreHiveConfig]() {}.getType
      } else if (storeType == StoreType.JANUS) {
        jsonType = new TypeToken[StoreJanusConfig]() {}.getType
      } else {
        logger.error(s"output format is [$storeType], is non supported, system exits")
        sys.exit()
      }
      val storeConfig: StoreConfig = JSONUtils.fromJson(JSONUtils.toJson(map.get(CoreConstants.STORE_CONFIG)), jsonType)
      val sinkConfig = SinkConfig(storeType, storeConfig)
      // sinks.add(buildSink(sinkConfig))
      sinks.add(SinkFactory.createSink(sinkConfig, schemaVo))
      if (storeType == StoreType.HIVE) {
        sinks.add(new FileHandleSink(storeType, sinkConfig.storeConfig.asInstanceOf[StoreHiveConfig]))
      }
    }
    sinks.toList
  }

  def getSchemoVo(): SchemaVo = {
    this.schemaVo
  }

  def getSinkStoreTypes(): Seq[StoreType] = {
    val sinks: List[Sink] = getSinks()
    sinks.map(sink => sink.storeType)
  }

  def getSinks(): List[Sink] = {
    this.sinkList
  }

  def getSink(storeType: StoreType*): Sink = {
    val sinks: List[Sink] = getSinks()
    for (sink <- sinks) {
      if (storeType.contains(sink.storeType)) {
        sink
      }
    }
    throw new RuntimeException("can't find Sink by StoreType=> " + storeType)
  }

  def getStoreAtlasConfig: StoreAtlasConfig = {
    this.getSink(StoreType.ATLAS, StoreType.GDB).asInstanceOf[StoreAtlasConfig]
  }

  def getStoreEsConfig: StoreEsConfig = {
    this.getSink(StoreType.ES).asInstanceOf[StoreEsConfig]
  }

  def getStoreHbaseConfig: StoreHBaseConfig = {
    this.getSink(StoreType.HBASE).asInstanceOf[StoreHBaseConfig]
  }

  def getStoreHiveConfig: StoreHiveConfig = {
    this.getSink(StoreType.HIVE).asInstanceOf[StoreHiveConfig]
  }

  def getStoreJanusConfig: StoreJanusConfig = {
    this.getSink(StoreType.JANUS).asInstanceOf[StoreJanusConfig]
  }

}
