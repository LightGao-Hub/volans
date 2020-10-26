package com.haizhi.volans.common.flink.base.scala.util

import org.apache.log4j.{Level, Logger}

object LogUtils {
  def setLevel(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  }
}
