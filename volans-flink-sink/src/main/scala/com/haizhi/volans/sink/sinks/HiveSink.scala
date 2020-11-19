package com.haizhi.volans.sink.sinks

import java.util.concurrent.TimeUnit

import com.haizhi.volans.sink.config.StoreHiveConfig
import com.haizhi.volans.sink.constant.StoreType
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.slf4j.LoggerFactory


/**
 * Author pengxb
 * Date 2020/11/16
 */
class HiveSink(override var storeType: StoreType, var storeConfig: StoreHiveConfig)
  extends Sink {

  private val logger = LoggerFactory.getLogger(classOf[HiveSink])

  // Todo
  private def buildHiveSink: StreamingFileSink[String] = {
    val streamSink = StreamingFileSink
      .forRowFormat(new Path(storeConfig.rollingPolicy.workDir), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new BasePathBucketAssigner[String] {
        override def getBucketId(element: String, context: BucketAssigner.Context): String = {
          val tokens = element.split(",")
          new StringBuilder()
            .append("country=")
            .append(tokens(tokens.length - 2))
            .append(java.io.File.separator)
            .append("province=")
            .append(tokens(tokens.length - 1)).toString()
        }
      })
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.SECONDS.toMillis(2))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
          .withMaxPartSize(1024)
          .build())
      .build()
    streamSink
  }

  override def build[T](v: T): RichSinkFunction[T] = {
    this.buildHiveSink.asInstanceOf[RichSinkFunction[T]]
  }
}
