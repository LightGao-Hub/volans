package com.haizhi.volans.sink.server

import java.util.Date

import com.haizhi.volans.sink.config.constant.HiveStoreType
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition, StorageDescriptor, Table}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

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

  def getTableLocation(database: String, table: String): String = {
    getTable(database, table).getSd.getLocation
  }

  def getStoreType(database: String, table: String): String = {
    getStoreType(getTable(database, table))
  }


  /**
   * 获取Hive表字段列表，不包括分区字段
   * @param table
   * @return List[(field,type)]
   */
  def getFieldSchema(table: Table): List[(String,String)] = {
    val cols = table.getSd.getCols
    val list = new ListBuffer[(String,String)]

    for(i <- 0 until cols.size()){
      list.append((cols.get(i).getName,cols.get(i).getType))
    }
    list.toList
  }

  /**
   * 获取Hive表字段列表，包括分区字段
   * @param table
   * @return
   */
  def getAllFieldSchema(table: Table): List[(String,String)] = {
    val cols = table.getSd.getCols
    val list = new ListBuffer[(String,String)]

    // 常规字段
    for(i <- 0 until cols.size()){
      list.append((cols.get(i).getName,cols.get(i).getType))
    }
    // 分区字段
    val partitionKeys = table.getPartitionKeys
    for(i <- 0 until partitionKeys.size()){
      list.append((partitionKeys.get(i).getName,partitionKeys.get(i).getType))
    }
    list.toList
  }

  /**
   * 获取Hive表字段map集合
   * @param table
   * @return
   */
  def getFieldSchemaMap(table: Table): java.util.Map[String,String] = {
    val cols = table.getSd.getCols
    val schemaMap = new java.util.HashMap[String,String]
    for(i <- 0 until cols.size()){
      schemaMap.put(cols.get(i).getName,cols.get(i).getType)
    }
    schemaMap
  }

  /**
   * 获取字段分隔符
   * @param table
   * @return
   */
  def getFieldDelimited(table: Table): String ={
    table.getSd.getSerdeInfo.getParameters.get("field.delim")
  }

  /**
   * 获取表存储格式
   *
   * @param table
   * @return
   */
  def getStoreType(table: Table): String = {
    var serLib = table.getSd.getSerdeInfo.getSerializationLib
    if (StringUtils.isBlank(serLib)) {
      serLib = table.getSd.getInputFormat
    }
    HiveStoreType.getStoreType(serLib)
  }

  /**
   * 获取分区列信息
   *
   * @param table
   * @return
   */
  def getPartitionKeys(table: Table): List[String] = {
    table.getPartitionKeys.toArray(Array[FieldSchema]()).toList.map(_.getName)
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
