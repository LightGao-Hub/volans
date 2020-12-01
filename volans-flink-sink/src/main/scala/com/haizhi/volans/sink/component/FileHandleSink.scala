package com.haizhi.volans.sink.component

import com.haizhi.volans.sink.combiner.{CombineFileJob, CommitFileJob}
import com.haizhi.volans.sink.config.constant.StoreType
import com.haizhi.volans.sink.config.store.StoreHiveConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.LoggerFactory

/**
 * Author pengxb
 * Date 2020/11/30
 */
class FileHandleSink(override var storeType: StoreType, var storeConfig: StoreHiveConfig)
  extends RichSinkFunction[Iterable[String]] with Sink {

  private val logger = LoggerFactory.getLogger(classOf[FileHandleSink])
  override var uid: String = "FileHandle"

  override def open(parameters: Configuration): Unit = {
    try {
      if (storeConfig.rollingPolicy.rolloverEnable) {
        // 启动文件合并线程
        CombineFileJob.init(storeConfig)
        val runnable = new Thread(CombineFileJob)
        runnable.setName("CombineFileJob")
        runnable.start()
      } else {
        // 启动文件提交线程，不合并文件
        CommitFileJob.init(storeConfig)
        val runnable = new Thread(CommitFileJob)
        runnable.setName("CommitFileJob")
        runnable.start()
      }
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  override def close(): Unit = {

  }

  override def build[T](v: T): RichSinkFunction[T] = {
    this.asInstanceOf[RichSinkFunction[T]]
  }

}
