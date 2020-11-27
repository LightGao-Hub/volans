package com.haizhi.volans.loader.scala.executor

trait LogExecutor extends Executor {
  //写入异常数据函数
  def LogWriter(value: String)
}
