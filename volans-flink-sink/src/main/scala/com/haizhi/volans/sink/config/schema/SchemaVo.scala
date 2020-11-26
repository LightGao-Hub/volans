package com.haizhi.volans.sink.config.schema

import com.haizhi.volans.sink.config.constant.Keys
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._

/**
 * Created by zhuhan on 2019/8/8.
 */
case class SchemaVo(name: String,
                    `type`: String,
                    fields: java.util.Map[String, SchemaFieldVo]) {

  def getScalaFields: Map[String, SchemaFieldVo] = {
    fields.toMap
  }

  def isVertex(): Boolean = {
    StringUtils.equalsIgnoreCase(Keys.VERTEX, `type`)
  }

  def isEdge(): Boolean = {
    !isVertex()
  }
}
