package com.haizhi.volans.sink.config.schema

import com.haizhi.volans.sink.config.constant.Keys
import org.apache.commons.lang3.StringUtils

/**
 * Created by zhuhan on 2019/8/8.
 */
case class SchemaFieldVo(sourceName: String,
                         targetName: String,
                         `type`: String,
                         isMain: String = "N") {

  def isMainField(): Boolean = {
    StringUtils.equalsIgnoreCase(isMain, Keys.Y)
  }
}