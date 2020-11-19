package com.haizhi.volans.sink.sinks

import com.haizhi.volans.sink.server.HiveJDBC
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author pengxb
 * Date 2020/11/12
 *
 * 定时刷新表分区
 * 目前Flink1.12及之前版本的Hive方言，没有实现[MSCK REPAIR TABLE table_name]DDL操作，所以暂时用JDBC连接实现
 */
class HiveTableRefreshJob(var url: String,
                          var name: String = null,
                          var password: String = null,
                          var tableName: String,
                          var scheduleInterval: Long = 60000)
  extends Runnable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[HiveTableRefreshJob])

  private val hiveJdbc: HiveJDBC = new HiveJDBC("")

  override def run(): Unit = {
    while (true) {
      logger.info("开始刷新分区...")
      //刷新表分区
      hiveJdbc.refreshTablePartition(tableName)
      logger.info("刷新完成...")
      Thread.sleep(scheduleInterval)
    }
  }

}
