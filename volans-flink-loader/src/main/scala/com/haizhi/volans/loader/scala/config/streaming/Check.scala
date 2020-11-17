package com.haizhi.volans.loader.scala.config.streaming

/**
 * sink根基类
 */
trait Check {
  /**
   * sink校验
   * @return
   */
  def check: Unit
}
