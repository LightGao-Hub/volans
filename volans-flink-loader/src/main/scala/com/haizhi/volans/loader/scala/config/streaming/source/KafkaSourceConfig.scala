package com.haizhi.volans.loader.scala.config.streaming.source

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.streaming.{Check, Config}
import com.hzxt.volans.loader.java.StoreType
import org.apache.commons.lang3.StringUtils

/**
 * @author gl 
 * @create 2020-11-02 17:17 
 */
case class KafkaSourceConfig(storeType: StoreType,
                             servers: String,
                             groupId: String,
                             topic: String) extends Check {
  //初始化校验
  check
  /**
   * source校验
   *
   * @return
   */
  override def check: Unit = {
    if (StringUtils.isBlank(servers) || StringUtils.isBlank(topic))
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK}  KafkaSourceConfig - servers/topic isEmpy")
  }
}
