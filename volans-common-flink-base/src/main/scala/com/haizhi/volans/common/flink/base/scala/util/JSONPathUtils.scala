package com.haizhi.volans.common.flink.base.scala.util

import com.jayway.jsonpath._

object JSONPathUtils {

  private val conf = Configuration.defaultConfiguration.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)

  val context: ParseContext = JsonPath.using(conf)

}
