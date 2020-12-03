package com.haizhi.volans.loader.scala.config.streaming.flink

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.check.CheckHelper
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.parameter.Parameter
import com.haizhi.volans.loader.scala.config.schema.Keys
import com.haizhi.volans.loader.scala.config.streaming.Check
import org.apache.commons.collections.MapUtils

/**
 * spark配置类
 *
 * @author gl
 * @create 2020-11-02 14:58
 */
case class FlinkConfig(var parallelism: Int = 3,
                       var checkpointInterval: Int = 1000 * 60,
                       var batchInterval: Int = 10000,
                       var restart: Int = 3,
                       checkPoint: String,
                       config: java.util.Map[String, Object] = null) extends Check {
  //初始化校验
  check

  /**
   * sink校验
   *
   * @return
   */
  override def check: Unit = {
    CheckHelper.checkNotNull(checkPoint, Parameter.CHECKPOINT)
    if (parallelism == 0)
      parallelism = 3
    if (checkpointInterval == 0)
      checkpointInterval = 1000 * 60
    if (batchInterval == 0)
      batchInterval = 10000
    if (restart == 0)
      restart = 3
  }
}
