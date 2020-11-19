package com.haizhi.volans.sink.key

import java.text.NumberFormat

/**
  * Created by zhuhan on 2019/8/8.
  */
object KeyUtils {
  /* Maximum logic partitions */
  val LOGIC_PARTITIONS = 1000
  /* Minimum Integer digits */
  val HASH_DIGITS = 3
  val GRAPH_ID_DIGITS = 3
  val SCHEMA_ID_DIGITS = 4
  private val formatter = NumberFormat.getNumberInstance

  def formatPlaceHolder(number: Long, digits: Int): String = {
    formatter.setMinimumIntegerDigits(digits)
    formatter.setGroupingUsed(false)
    formatter.format(number)
  }
}
