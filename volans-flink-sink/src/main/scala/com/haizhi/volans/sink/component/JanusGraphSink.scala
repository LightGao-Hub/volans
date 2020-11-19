package com.haizhi.volans.sink.component

import com.haizhi.volans.sink.config.constant.StoreType
import com.haizhi.volans.sink.config.store.StoreJanusConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * Author pengxb
 * Date 2020/11/18
 */
class JanusGraphSink(override var storeType: StoreType, var storeConfig: StoreJanusConfig)
  extends RichSinkFunction[Iterable[String]] with Sink {

  override def open(parameters: Configuration): Unit = {
  }

  override def invoke(value: Iterable[String], context: SinkFunction.Context[_]): Unit = {
  }

  override def close(): Unit = {
  }

  override def build[T](v: T): RichSinkFunction[T] = {
    this.asInstanceOf[RichSinkFunction[T]]
  }

}
