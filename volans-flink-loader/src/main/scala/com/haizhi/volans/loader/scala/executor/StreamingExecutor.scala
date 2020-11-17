package com.haizhi.volans.loader.scala.executor

/**
 * 流式执行配置类
 * @param errorExecutor 异常执行器
 */
case class StreamingExecutor(errorExecutor: ErrorExecutor,
                             dirtyExecutor: DirtyExecutor) {

  /**
   * 循环启动
   */
  def forInit(): Unit = {
    errorExecutor.init()
    dirtyExecutor.init()
  }

  /**
   * 循环关闭
   */
  def forClose(): Unit = {
    errorExecutor.close()
    dirtyExecutor.close()
  }

}
