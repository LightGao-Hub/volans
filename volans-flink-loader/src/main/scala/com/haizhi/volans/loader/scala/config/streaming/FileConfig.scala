package com.haizhi.volans.loader.scala.config.streaming

import com.haizhi.volans.loader.scala.config.streaming.error._
import org.apache.commons.lang3.StringUtils

/**
 * 文件配置类
 *
 * @author gl
 * @create 2020-11-02 15:02
 */
case class FileConfig(path: String,
                      var timeInterval: Long = 60) extends LogInfoConfig with DirtyConfig {
  /**
   * 判断path 必填项是否为空
   * @return
   */
  override def isEmpy: Boolean = {
    if(StringUtils.isBlank(path))
      true
    else {
      if(timeInterval == 0)
        timeInterval = 60
      false
    }
  }
}
