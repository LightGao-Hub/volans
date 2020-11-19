package com.haizhi.volans.sink.config.schema

import com.haizhi.volans.sink.config.constant.Keys
import org.apache.commons.lang3.StringUtils

/**
  * Created by zhuhan on 2019/8/8.
  */
case class SchemaFieldVo(id: java.lang.Long,
                         name: String,
                         `type`: String,
                         format: String,
                         isMain: String = "N",
                         required: String) {

  def isMainField(): Boolean = {
    StringUtils.equalsIgnoreCase(isMain, Keys.Y)
  }
}