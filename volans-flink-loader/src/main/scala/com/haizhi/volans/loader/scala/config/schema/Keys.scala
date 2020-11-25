package com.haizhi.volans.loader.scala.config.schema

import com.haizhi.volans.loader.scala.config.streaming.error.ErrorSink

/**
 * source端数据属性集
 */
object Keys {
  val OBJECT_KEY: String = "object_key"
  val TYPE_ERROR: String = "TYPE_ERROR"
  val FROM_KEY: String = "from_key"
  val TO_KEY: String = "to_key"
  val DEFAULT: String = ""
  val FAMILY: String = "objects"
  val _ROW_KEY: String = "_row_key"
  val VERTEX: String = "vertex"
  val EDGE: String = "edge"
  val EVENT_DETAIL: String = "event_detail"
  val Y: String = "Y"
  val TABLE_NAME = "table_name"
  val ID = "id"
  val LABEL = "label"
  val LABEL_V = "label_v"
  val LABEL_E = "label_e"
  val KEY = "key"

  val CHECK_ERROR = "CHECK_ERROR"
  val RUNTIME_ERROR = "RUNTIME_ERROR"
  val DIRTY_LOG = "dirty.log"
  val ERROR_LOG = "error.log"
  val COUNT_LOG = "count.log"
  val PATH_FLAG = "/"
  val FILE_PATH = "file:///"
  val HDFS_PATH = "hdfs://"
  val ENCODING = "utf-8"

  //taskInstanceId参数意义是在检查参数期间将异常能够输出，因为前端展示异常需要taskInstanceId参数
  var taskInstanceId: String = ""
  //affected_store参数是异常中处理脏数据的下游sinks以逗号分割的字符串，供前端展示
  var affected_store: String = ""
  //寄存errorSink，为了解决异常信息打印
  var errorSink: ErrorSink = _
}
