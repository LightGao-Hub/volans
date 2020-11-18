package com.haizhi.volans.loader.scala.config.streaming.dirty

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.check.CheckHelper
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.parameter.Parameter
import com.haizhi.volans.loader.scala.config.streaming.Check
import com.hzxt.volans.loader.java.StoreType

/**
 * 脏数据处理sink
 * errorMode: --错误处理方式，-1表示出错继续，大于或等于0表示错误行大于该值终止
 * errorStoreEnabled: --是否开启错误信息记录
 * errorStoreRowsLimit: --限制错误信息记录条数
 *
 * @author gl
 * @create 2020-11-02 15:07
 */
case class DirtySink(storeType: StoreType,
                     errorMode: Long = -1,
                     errorStoreEnabled: Boolean = false,
                     errorStoreRowsLimit: Long = 30000,
                     inboundTaskId: String = null,
                     taskInstanceId: String = null,
                     dirtyConfig: DirtyConfig) extends Check {
  //初始化校验
  check

  /**
   * sink校验
   *
   * @return
   */
  override def check: Unit = {
    CheckHelper.checkNotNull(taskInstanceId, Parameter.TASK_INSTANCEID)
    if (dirtyConfig.isEmpy)
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK}  dirtySink - dirtyConfig isEmpy")
  }
}
