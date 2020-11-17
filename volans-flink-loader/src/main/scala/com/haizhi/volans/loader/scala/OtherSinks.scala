package com.haizhi.volans.loader.scala

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.slf4j.{Logger, LoggerFactory}

/**
 * 其他sinks 自定义类
 *
 * @author gl
 */
class OtherSinks extends RichSinkFunction[Iterable[String]] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[OtherSinks])

  override def open(parameters: Configuration): Unit = {
    logger.info(" OtherSinks open ")
  }

  override def invoke(value: Iterable[String], context: Context[_]): Unit = {
    logger.info(s" OtherSinks invoke value = ${value} ")
  }

  override def close(): Unit = {
    logger.info(" OtherSinks close ")
  }

}
