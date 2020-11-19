package com.haizhi.volans.sink.config.schema

/**
 * Author pengxb
 * Date 2020/11/4
 */
case class RollingPolicyVo(workDir: String,
                           rolloverEnable: Boolean = false,
                           maxPartSize: Long = 128 * 1024 * 1024,
                           rolloverInterval: Long = 60,
                           inactivityInterval: Long = 60,
                           rolloverCheckInterval: Long = 60
                          )
