package com.haizhi.volans.loader.scala.config.schema

import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._

/**
 * @author gl 
 * @create 2020-11-02 13:43
 */
case class SchemaVo(name: String,
                    `type`: String,
                    fields: java.util.List[SchemaFieldVo]) {


  def isVertex: Boolean = {
    StringUtils.equalsIgnoreCase("vertex", `type`)
  }

  def isEdge: Boolean = {
    StringUtils.equalsIgnoreCase("edge", `type`)
  }
}
