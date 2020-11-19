package com.haizhi.volans.sink.config

/**
  * Create by zhoumingbing on 2020-08-06
  */
trait StoreConfig {

  def getGraph: String

  def getSchema: String

}
