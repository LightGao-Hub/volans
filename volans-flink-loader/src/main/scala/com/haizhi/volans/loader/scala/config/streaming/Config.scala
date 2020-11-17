package com.haizhi.volans.loader.scala.config.streaming

/**
 * 所有config根基接口
 * @author gl 
 * @create 2020-11-03 16:54 
 */
trait Config {

  /**
   * 判断Dirty 必填项是否为空
   * @return
   */
  def isEmpy: Boolean

}
