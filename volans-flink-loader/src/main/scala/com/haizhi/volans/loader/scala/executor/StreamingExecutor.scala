package com.haizhi.volans.loader.scala.executor

/**
 * 流式执行配置类
 */
case class StreamingExecutor(logExecutor: LogExecutor,
                             dirtyExecutor: DirtyExecutor) {

  /**
   * 循环启动
   */
  def forInit(): Unit = {
    logExecutor.init()
    dirtyExecutor.init()
  }

  /**
   * 循环关闭
   */
  def forClose(): Unit = {
    logExecutor.close()
    dirtyExecutor.close()
  }

}
