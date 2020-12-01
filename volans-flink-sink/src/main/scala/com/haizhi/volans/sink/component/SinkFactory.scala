package com.haizhi.volans.sink.component

import com.haizhi.volans.sink.config.constant.StoreType
import com.haizhi.volans.sink.config.schema.SchemaVo
import com.haizhi.volans.sink.config.store.{SinkConfig, StoreAtlasConfig, StoreEsConfig, StoreHBaseConfig, StoreHiveConfig, StoreJanusConfig}

/**
 * Author pengxb
 * Date 2020/11/28
 */
object SinkFactory {

  def createSink(sinkConfig: SinkConfig, schemaVo: SchemaVo): Sink = {
    val storeType: StoreType = sinkConfig.storeType
    storeType match {
      case StoreType.ATLAS | StoreType.GDB =>
        new ArangoDBSink(storeType, sinkConfig.storeConfig.asInstanceOf[StoreAtlasConfig], schemaVo)
      case StoreType.ES =>
        new RestEsSink(storeType, sinkConfig.storeConfig.asInstanceOf[StoreEsConfig])
      case StoreType.HBASE =>
        new HbaseSink(storeType, sinkConfig.storeConfig.asInstanceOf[StoreHBaseConfig], schemaVo)
      case StoreType.JANUS =>
        new JanusGraphSink(storeType, sinkConfig.storeConfig.asInstanceOf[StoreJanusConfig])
      case StoreType.HIVE =>
        val hiveSink = new HiveSink(storeType, sinkConfig.storeConfig.asInstanceOf[StoreHiveConfig], schemaVo)
        hiveSink.init()
        hiveSink
      case _ => throw new RuntimeException(">>>data out format error")
    }
  }

}
