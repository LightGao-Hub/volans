package com.haizhi.volans.loader.scala.config.schema

import org.apache.commons.lang3.StringUtils

/**
  * Created by zhuhan on 2019/8/8.
  */
case class SchemaFieldVo(name: String,
                         `type`: String,
                         var isMain: String = "N") {

  def isMainField: Boolean = {
    if(StringUtils.isBlank(isMain))
      isMain = "N"
    StringUtils.equalsIgnoreCase(isMain, "Y")
  }

}
