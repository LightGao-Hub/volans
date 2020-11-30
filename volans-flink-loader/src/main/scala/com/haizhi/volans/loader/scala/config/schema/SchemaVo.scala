package com.haizhi.volans.loader.scala.config.schema

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.check.CheckHelper
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.parameter.Parameter
import com.haizhi.volans.loader.scala.config.streaming.Check
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._

/**
 * @author gl 
 * @create 2020-11-02 13:43
 */
case class SchemaVo(name: String,
                    `type`: String,
                    var operation: String,
                    fields: java.util.List[SchemaFieldVo]) extends Check  {
  //初始化校验
  check

  /**
   * sink校验
   *
   * @return
   */
  override def check: Unit = {
    CheckHelper.checkNotNull(`type`, Parameter.TYPE)
    if (StringUtils.isBlank(operation))
      operation = "_operation"
  }

  def isVertex: Boolean = {
    StringUtils.equalsIgnoreCase("vertex", `type`)
  }

  def isEdge: Boolean = {
    StringUtils.equalsIgnoreCase("edge", `type`)
  }
}
