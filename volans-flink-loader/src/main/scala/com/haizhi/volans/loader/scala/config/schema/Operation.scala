package com.haizhi.volans.loader.scala.config.schema

import com.haizhi.volans.loader.scala.config.check.CheckHelper
import com.haizhi.volans.loader.scala.config.streaming.Check
import com.hzxt.volans.loader.java._

case class Operation(mode: String, var operateField: String) extends Check {
  /**
   * sink校验
   *
   * @return
   */
  override def check: Unit = {
    CheckHelper.checkNotNull(mode, "Operation mode")
    val operateMode: OperationMode = OperationMode.findStoreType(mode)
    if(operateMode == OperationMode.MIX)
      CheckHelper.checkNotNull(operateField, " operateField is null ")
  }

  def isMix: Boolean = {
    val operateMode: OperationMode = OperationMode.findStoreType(mode)
    operateMode == OperationMode.MIX
  }
}
