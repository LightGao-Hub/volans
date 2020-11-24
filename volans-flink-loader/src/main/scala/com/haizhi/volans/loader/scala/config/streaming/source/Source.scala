package com.haizhi.volans.loader.scala.config.streaming.source

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.streaming.Check
import com.hzxt.volans.loader.java.StoreType
import org.apache.commons.lang3.StringUtils

case class Source(storeType: StoreType,
                  kafkaSourceConfig: KafkaSourceConfig) extends Check {
  //初始化校验
  check
  /**
   * source校验
   *
   * @return
   */
  override def check: Unit = {
    if (StringUtils.isBlank(kafkaSourceConfig.servers) || StringUtils.isBlank(kafkaSourceConfig.topic))
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK}  KafkaSourceConfig - servers/topic isEmpy")
    if (StringUtils.isBlank(kafkaSourceConfig.groupId))
      kafkaSourceConfig.groupId = "flink-loader"
  }
}
