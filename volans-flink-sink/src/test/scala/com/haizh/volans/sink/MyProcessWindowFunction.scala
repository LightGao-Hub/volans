package com.haizh.volans.sink

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Author pengxb
 * Date 2020/11/18
 */
class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), Iterable[String], Int, TimeWindow] {
  override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Iterable[String]]): Unit = {
    out.collect(elements.map(_._1))
  }
}
