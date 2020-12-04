package com.haizhi.volans.sink.component

import com.haizhi.volans.sink.combiner.CombineFileJobV2
import com.haizhi.volans.sink.config.constant.StoreType
import com.haizhi.volans.sink.config.store.StoreHiveConfig
import com.haizhi.volans.sink.server.HiveDao
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

/**
 * Author pengxb
 * Date 2020/11/30
 */
class FileHandleSink(override var storeType: StoreType, var storeConfig: StoreHiveConfig)
  extends RichSinkFunction[Iterable[String]] with Sink {

  private val logger = LoggerFactory.getLogger(classOf[FileHandleSink])
  override var uid: String = "FileHandle"
  private var hiveDao: HiveDao = _
  private var combineFileJob: CombineFileJobV2 = _
  // 上次执行文件合并Task的时间
  private var lastExecutionTimeStamp: Long = _
  // 文件合并Task检查周期
  private var rolloverCheckInterval: Long = _

  override def open(parameters: Configuration): Unit = {
    this.hiveDao = new HiveDao()
    this.lastExecutionTimeStamp = System.currentTimeMillis()
    logger.info(s"FileHandleSink初始化时间: $lastExecutionTimeStamp")
    this.rolloverCheckInterval = storeConfig.rollingPolicy.rolloverCheckInterval * 1000
    this.combineFileJob = new CombineFileJobV2(hiveDao, storeConfig)
    combineFileJob.init()
  }

  override def invoke(value: Iterable[String], context: SinkFunction.Context[_]): Unit = {
    val currentTimeStamp = System.currentTimeMillis
    val interval = currentTimeStamp - this.lastExecutionTimeStamp
    logger.info(s"当前时间: $currentTimeStamp, 距离上次执行任务时间: $interval")
    if (interval >= this.rolloverCheckInterval) {
      // 执行文件合并和提交分区操作
      logger.info(s"距离上次执行任务超过${rolloverCheckInterval}毫秒, 开始执行文件合并操作...")
      combineFileJob.runTask()
      this.lastExecutionTimeStamp = currentTimeStamp
    }
  }

  override def close(): Unit = {
    hiveDao.shutdown()
  }

  override def build[T](v: T): RichSinkFunction[T] = {
    this.asInstanceOf[RichSinkFunction[T]]
  }

}
