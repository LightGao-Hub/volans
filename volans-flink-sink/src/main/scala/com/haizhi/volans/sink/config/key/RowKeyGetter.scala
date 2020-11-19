package com.haizhi.volans.sink.config.key

import java.text.MessageFormat

/**
  * Created by zhuhan on 2019/8/8.
  */
object RowKeyGetter {

  def getRowKey(objectKey: String): String = {
    MessageFormat.format("{0}#{1}", getPartitionHash(objectKey), objectKey)
  }

  ///////////////////////
  // private functions
  ///////////////////////
  private def getPartitionHash(md5: String): String = {
    val remainder = java.lang.Long.remainderUnsigned(Math.abs(md5.hashCode), KeyUtils.LOGIC_PARTITIONS)
    KeyUtils.formatPlaceHolder(remainder, KeyUtils.HASH_DIGITS)
  }

  private def toLong(md5: String): Long = {
    var h = 0
    for (i <- 0 until md5.length) {
      h = 127 * h + md5.charAt(i)
    }
    h
  }
}
