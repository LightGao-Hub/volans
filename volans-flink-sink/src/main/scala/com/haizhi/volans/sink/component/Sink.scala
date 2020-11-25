package com.haizhi.volans.sink.component

import java.util.Properties

import com.haizhi.volans.sink.config.constant.StoreType
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * Author pengxb
 * Date 2020/11/16
 */
trait Sink{

  // Sink标识
  var uid: String

  // 存储格式
  var storeType: StoreType

  // config容器
  var config: Properties = new Properties()

  def build[T](v: T): RichSinkFunction[T]

}
