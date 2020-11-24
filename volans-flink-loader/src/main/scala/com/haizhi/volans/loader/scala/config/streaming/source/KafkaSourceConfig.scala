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
case class KafkaSourceConfig(servers: String,
                             var groupId: String,
                             topic: String,
                             config: java.util.Map[String, Object] = null) {


}
