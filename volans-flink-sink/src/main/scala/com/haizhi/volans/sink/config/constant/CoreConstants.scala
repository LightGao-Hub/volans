package com.haizhi.volans.sink.config.constant

/**
  * Create by zhoumingbing on 2020-08-13
  */
object CoreConstants {
  val OPERATION_UPSERT: String = "UPSERT"
  val OPERATION_DELETE: String = "DELETE"
  val OPERATION_REBUILD: String = "REBUILD"
  val OPERATION_INSERT: String = "INSERT"

  val VERSION: String = "version"
  val OPERATION: String = "operation"
  val SOURCE: String = "source"
  val SINKS: String = "sinks"
  val LOG_SINK: String = "logSink"
  val SINK: String = "sink"
  val GAP_CONFIG: String = "gapConfig"
  val SPARK_CONFIG: String = "sparkConfig"
  val STORE_TYPE: String = "storeType"
  val STORE_CONFIG: String = "storeConfig"
  val SCHEMA: String = "schema"
}
