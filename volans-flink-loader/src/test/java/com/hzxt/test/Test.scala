package com.hzxt.test

import com.haizhi.volans.loader.scala.StartFlinkLoader.{StartFlinkLoader, errorExecutor, logger, streamingConfig, streamingExecutor}
import com.haizhi.volans.loader.scala.config.check.{StreamingConfigHelper, StreamingExecutorHelper}

object Test {

  def main(args: Array[String]): Unit = {
    initArgsExecutor(args)
  }

  /**
   * 加载全局参数
   */
  def initArgsExecutor(args: Array[String]): Unit = {
    val args: Array[String] = Array("/Users/hzxt/project/IDEA_HZXT/volans-flink/volans/volans-flink-loader/src/main/resources/conf/参数配置.json")
    //加载全局参数
    streamingConfig = StreamingConfigHelper.parse(args)
    //构建streamingExecutor
    streamingExecutor = StreamingExecutorHelper.buildExecutors(streamingConfig)
    errorExecutor = streamingExecutor.errorExecutor
    //循环初始化
    streamingExecutor.forInit()
  }

}
