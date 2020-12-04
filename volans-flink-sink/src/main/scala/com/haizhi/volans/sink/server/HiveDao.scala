package com.haizhi.volans.sink.server

import java.util.Date

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{Partition, Table}
import org.slf4j.LoggerFactory

/**
 * Author pengxb
 * Date 2020/11/3
 */
class HiveDao extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)

  private var client = new HiveMetaStoreClient(new HiveConf())

  def getClient(): HiveMetaStoreClient = {
    client
  }

  def getTable(database: String, table: String): Table = {
    client.getTable(database, table)
  }

  /**
   * 获取分区信息
   *
   * @param database  数据库
   * @param table     表
   * @param fiterExpr 过滤条件
   * @param maxPart   返回分区数目
   * @return
   */
  def listPartitionByFilter(database: String, table: String, fiterExpr: String, maxPart: Short): java.util.List[Partition] = {
    client.listPartitionsByFilter(database, table, fiterExpr, maxPart)
  }

  /**
   * 获取分区信息
   *
   * @param table
   * @param fiterExpr
   * @param maxPart
   * @return
   */
  def listPartitionByFilter(table: Table, fiterExpr: String, maxPart: Short): java.util.List[Partition] = {
    listPartitionByFilter(table.getDbName, table.getTableName, fiterExpr, maxPart)
  }

  /**
   * 添加分区
   *
   * @param table             表描述信息
   * @param values            分区值列表，按partitionKey顺序存入，例如：'{"year","month","day"}'
   * @param partitionLocation 分区HDFS目录名称，例如：'/year=2020/month=11/day=11'
   */
  def addPartition(table: Table, values: java.util.List[String], partitionLocation: String): Unit = {
    val partition = new Partition()
    val partitionSd = table.getSd.deepCopy()
    partitionSd.setLocation(table.getSd.getLocation + partitionLocation)
    partition.setSd(partitionSd)
    partition.setValues(values)
    partition.setDbName(table.getDbName)
    partition.setTableName(table.getTableName)
    partition.setCreateTime((new Date().getTime / 1000).toInt)
    partition.setLastAccessTime(0)
    logger.info(s"添加分区$partition")
    client.add_partition(partition)
  }

  /**
   * 添加分区
   *
   * @param database          数据库
   * @param table             表
   * @param values            分区值列表，按partitionKey顺序存入，例如：'{"year","month","day"}'
   * @param partitionLocation 分区HDFS目录名称，例如：'/year=2020/month=11/day=11'
   */
  def addPartition(database: String, table: String, values: java.util.List[String], partitionLocation: String): Unit = {
    val _table = client.getTable(database, table)
    this.addPartition(_table, values, partitionLocation)
  }

  def shutdown(): Unit = {
    try {
      if (client != null) {
        client.close()
      }
    } catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
        client = null
      }
    }
  }

}
