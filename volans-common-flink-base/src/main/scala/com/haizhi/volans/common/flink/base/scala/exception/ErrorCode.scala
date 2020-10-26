package com.haizhi.volans.common.flink.base.scala.exception

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author gl 
 * @create 2020-11-03 17:53 
 */
object ErrorCode {

  val PATH_BREAK = "##"
  val PARAMETER_CHECK_ERROR:String = "CHECK_ERROR"
  val STREAMING_ERROR:String = "STREAMING_ERROR"

  /**
   *
   * @param id  启动时异常默认时间戳，脏数据时为object_key + _ + affected_store
   * @param object_key  启动时异常默认为空，脏数据时为object_key
   * @param task_instance_id dirty检测无问题时为全局参数，有问题时无参
   * @param code  错误类型，例如：check_error , upsert_error
   * @param message  错误信息
   * @param affected_store
   * @param data_row
   * @param updated_dt
   * @return
   */
  def toJSON(id: String = LongDate(), object_key: String = "", task_instance_id: String = "", code: String = "", message: String = "", affected_store: String = "", data_row: String = "", updated_dt: String = NowDate()): String = {
    if(message.contains(s"$PATH_BREAK"))
      JSONUtils.toJson(ErrorMessage(id, object_key, task_instance_id, message.split(s"$PATH_BREAK")(0), message.split(s"$PATH_BREAK")(1), affected_store, data_row, updated_dt))
    else
      JSONUtils.toJson(ErrorMessage(id, object_key, task_instance_id, code, message, affected_store, data_row, updated_dt))
  }

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(now)
  }

  def LongDate(): String = {
    new Date().getTime.toString
  }

}
