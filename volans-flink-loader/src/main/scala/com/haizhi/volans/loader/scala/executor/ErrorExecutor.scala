package com.haizhi.volans.loader.scala.executor

trait ErrorExecutor extends Executor {
  //写入异常数据函数
  def errorWriter(value: String)
}
