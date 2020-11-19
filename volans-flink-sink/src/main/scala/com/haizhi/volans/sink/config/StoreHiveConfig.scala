package com.haizhi.volans.sink.config

/**
 * Create by zhoumingbing on 2020-08-13
 */
case class StoreHiveConfig(database: String,
                           table: String,
                           user: String,
                           password: String,
                           delField: String,
                           delFieldFlag: String,
                           rollingPolicy: RollingPolicyVo
                          ) extends StoreConfig {
  override def getGraph: String = {
    database
  }

  override def getSchema: String = {
    table
  }
}
