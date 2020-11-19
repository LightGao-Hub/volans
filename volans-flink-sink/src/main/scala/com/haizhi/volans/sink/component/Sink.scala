package com.haizhi.volans.sink.component

import com.haizhi.volans.sink.config.constant.StoreType
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * Author pengxb
 * Date 2020/11/16
 */
trait Sink{

  // Sink标识
  var uid: String

  var storeType: StoreType

  def build[T](v: T): RichSinkFunction[T]

}
