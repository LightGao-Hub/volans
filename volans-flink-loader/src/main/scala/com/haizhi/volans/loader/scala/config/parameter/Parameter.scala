package com.haizhi.volans.loader.scala.config.parameter

/**
 * 全局参数关键字
 *
 * @author gl
 * @create 2020-11-02 14:51
 */
object Parameter {

  val STORE_TYPE: String = "storeType"
  val SOURCE: String = "source"
  val SOURCE_CONFIG: String = "sourceConfig"
  val SINKS: String = "sinks"
  val SCHEMA: String = "schema"
  val DIRTY_SINK: String = "dirtySink"
  val DIRTY_CONFIG: String = "dirtyConfig"
  val ERROR_SINK: String = "errorSink"
  val ERROR_CONFIG: String = "errorConfig"
  val COUNT_SINK: String = "countSink"
  val COUNT_CONFIG: String = "countConfig"
  val CHECKPOINT: String = "checkPoint"
  val STPO_VOLANS_PATH: String = "stopVolansPath"
  val FLINK_CONFIG: String = "flinkConfig"
  val STORE_CONFIG: String = "storeConfig"

  val FIELDS: String = "fields"
  val NAME: String = "name"
  val TYPE: String = "type"
  val IS_MAIN: String = "isMain"

  val ERROR_MODE: String = "errorMode"
  val ERROR_STORE_ENABLED: String = "errorStoreEnabled"
  val ERROR_STORE_FOWSLIMIT: String = "errorStoreRowsLimit"
  val INBOUND_TASKID: String = "inboundTaskId"
  val TASK_INSTANCEID: String = "taskInstanceId"
  val PATH: String = "path"
  val KAFKA_SERVER: String = "servers"
  val KAFKA_GROUPID: String = "groupId"
  val TOPIC: String = "topic"

}
