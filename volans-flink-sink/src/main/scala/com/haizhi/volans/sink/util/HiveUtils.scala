package com.haizhi.volans.sink.util

import com.haizhi.volans.sink.config.constant.HiveStoreType
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition, Table}

import scala.collection.mutable.ListBuffer

/**
 * Author pengxb
 * Date 2020/12/3
 */
object HiveUtils {

  /**
   * 获取表存储类型
   *
   * @param serLib 序列化字符串
   * @return
   */
  def getTableStoredType(serLib: String): String = {
    if (serLib.contains("io.parquet")) {
      HiveStoreType.PARQUET
    } else if (serLib.contains("io.orc")) {
      HiveStoreType.ORC
    } else if (serLib.contains("io.rcfile")) {
      HiveStoreType.RCFILE
    } else if (serLib.contains("io.avro")) {
      HiveStoreType.AVRO
    } else {
      HiveStoreType.TEXTFILE
    }
  }

  def getTableStoredType(table: Table): String = {
    var serLib = table.getSd.getSerdeInfo.getSerializationLib
    if (StringUtils.isBlank(serLib)) {
      serLib = table.getSd.getInputFormat
    }
    getTableStoredType(serLib)
  }

  /**
   * 获取表存储路径
   *
   * @param table
   * @return
   */
  def getTableLocation(table: Table): String = {
    table.getSd.getLocation
  }

  /**
   * 获取字段分隔符
   *
   * @param table
   * @return
   */
  def getFieldDelimited(table: Table): String = {
    table.getSd.getSerdeInfo.getParameters.get("field.delim")
  }

  /**
   * 获取分区schema信息
   *
   * @param table
   * @return
   */
  def getPartitionSchema(table: Table): List[(String, String)] = {
    table.getPartitionKeys
      .toArray(Array[FieldSchema]())
      .toList
      .map(schema => (schema.getName, schema.getType))
  }

  /**
   * 获取Hive表字段map集合
   *
   * @param table
   * @return
   */
  def getFieldSchemaMap(table: Table): java.util.Map[String, String] = {
    val cols = table.getSd.getCols
    val schemaMap = new java.util.HashMap[String, String]
    for (i <- 0 until cols.size()) {
      schemaMap.put(cols.get(i).getName, cols.get(i).getType)
    }
    schemaMap
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
   * 获取分区存储路径
   *
   * @param partition
   * @return
   */
  def getPartitionLocation(partition: Partition): String = {
    partition.getSd.getLocation
  }

  /**
   * 获取Hive表字段列表，不包括分区字段
   *
   * @param table
   * @return List[(field,type)]
   */
  def getFieldSchema(table: Table): List[(String, String)] = {
    val cols = table.getSd.getCols
    val list = new ListBuffer[(String, String)]

    for (i <- 0 until cols.size()) {
      list.append((cols.get(i).getName, cols.get(i).getType))
    }
    list.toList
  }

  /**
   * 获取Hive表字段列表，包括分区字段
   *
   * @param table
   * @return
   */
  def getAllFieldSchema(table: Table): List[(String, String)] = {
    val cols = table.getSd.getCols
    val list = new ListBuffer[(String, String)]
    // 常规字段
    for (i <- 0 until cols.size()) {
      list.append((cols.get(i).getName, cols.get(i).getType))
    }
    // 分区字段
    val partitionKeys = table.getPartitionKeys
    for (i <- 0 until partitionKeys.size()) {
      list.append((partitionKeys.get(i).getName, partitionKeys.get(i).getType))
    }
    list.toList
  }

}
