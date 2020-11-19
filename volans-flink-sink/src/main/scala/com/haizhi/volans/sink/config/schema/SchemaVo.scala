package com.haizhi.volans.sink.config.schema

import com.haizhi.volans.sink.config.constant.Keys
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._

/**
  * Created by zhuhan on 2019/8/8.
  */
case class SchemaVo(id: java.lang.Long,
                    graphId: java.lang.Long,
                    name: String,
                    `type`: String,
                    useGdb: String,
                    useSearch: String,
                    fields: java.util.Map[String, SchemaFieldVo]) {

  def getScalaFields: Map[String, SchemaFieldVo] = {
    fields.toMap
  }

  def isVertex(): Boolean = {
    StringUtils.equalsIgnoreCase(Keys.VERTEX, `type`)
  }

  def isEdge():Boolean ={
    !isVertex()
  }
}
