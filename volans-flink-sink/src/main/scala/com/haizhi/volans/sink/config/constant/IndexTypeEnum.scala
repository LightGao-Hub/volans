package com.haizhi.volans.sink.config.constant

/**
  * Created by zhuhan on 2019/8/8.
  */
object IndexTypeEnum extends Enumeration {
  type IndexTypeEnum = Value
  val HASHINDEX = Value("0")
  val FULLTEXTINDEX = Value("1")
  val GEOINDEX = Value("2")
  val PERSISTENTINDEX = Value("3")
  val SKIPLISTINDEX = Value("4")
}
