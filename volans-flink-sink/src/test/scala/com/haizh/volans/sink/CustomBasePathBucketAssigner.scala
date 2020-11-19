package com.haizh.volans.sink

import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner

/**
 * Author pengxb
 * Date 2020/11/12
 */
class CustomBasePathBucketAssigner extends BasePathBucketAssigner[String]{

  /**
   * 自定义分区目录
   * @param element
   * @param context
   * @return
   */
  override def getBucketId(element: String, context: BucketAssigner.Context): String = {
    val tokens = element.split(",")
    new StringBuilder()
      .append("country=")
      .append(tokens(tokens.length-2))
      .append(java.io.File.separator)
      .append("province=")
      .append(tokens(tokens.length-1)).toString()
  }

}
