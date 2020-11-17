package com.haizhi.volans.loader.scala.executor

trait DirtyExecutor extends Executor {
  //写入脏数据函数
  def dirtyWriter(value: String)
}
