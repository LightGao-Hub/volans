package com.haizhi.volans.sink.server

import java.sql.{Connection, DriverManager}

import org.slf4j.{Logger, LoggerFactory}

/**
 * Author pengxb
 * Date 2020/11/12
 */
class HiveJDBC(var url: String, var name: String = null, var password: String=null) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[HiveJDBC])

  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  {
    Class.forName(driverName)
  }

  def getConnection(): Connection = {
    DriverManager.getConnection(url, name, password)
  }

  def getConnection(url: String, name: String, password: String): Connection = {
    DriverManager.getConnection(url, name, password)
  }

  def executeDDL(sql: String): Int = {
    val conn = getConnection()
    val stmt = conn.createStatement()
    val resultCnt = stmt.executeUpdate(sql)
    close(stmt, conn)
    resultCnt
  }

  def close(resources: AutoCloseable*): Unit = {
    resources.foreach(resource => {
      try {
        if (resource != null) {
          resource.close()
        }
      } catch {
        case e: Exception => logger.error(e.getMessage, e)
      }
    })
  }

  /**
   * 刷新分区信息
   *
   * @param tableName
   * @return
   */
  def refreshTablePartition(tableName: String): Unit = {
    logger.info(s"MSCK REPAIR TABLE $tableName")
    executeDDL(s"MSCK REPAIR TABLE $tableName")
  }

}
