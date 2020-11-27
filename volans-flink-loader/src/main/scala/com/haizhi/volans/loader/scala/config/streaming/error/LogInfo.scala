package com.haizhi.volans.loader.scala.config.streaming.error

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.streaming.Check
import com.hzxt.volans.loader.java.StoreType

/**
 * 异常信息sink
 *
 * @author gl
 * @create 2020-11-02 15:03
 */
case class LogInfo(storeType: StoreType, var logInfoConfig: LogInfoConfig) extends Check {
  //初始化校验
  check
  /**
   * sink校验
   *
   * @return
   */
  override def check: Unit = {
    if (logInfoConfig.isEmpy)
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK}  errorSink - errorConfig isEmpy")
  }
}
