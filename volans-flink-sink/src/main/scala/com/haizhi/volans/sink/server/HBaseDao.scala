package com.haizhi.volans.sink.server

import java.io.{Closeable, IOException}
import java.util

import com.google.gson.{JsonObject, JsonPrimitive}
import com.haizhi.volans.sink.config.store.StoreHBaseConfig
import com.haizhi.volans.sink.config.constant.{Keys, StoreType}
import com.haizhi.volans.sink.config.key.RowKeyPartitioner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

/**
 * Created by zhuhan on 2019/8/8.
 */
class HBaseDao extends Serializable {

  val logger = LoggerFactory.getLogger(getClass)

  private var config: Configuration = _
  private var connection: Connection = _
  private var admin: HBaseAdmin = _
  private val DEFAULT: String = "default"
  private val DEFAULT_FAMILY = Keys.FAMILY

  def init(hBaseConfig: StoreHBaseConfig): Unit = {
    try {
      val ipPortAry: Array[String] = hBaseConfig.url.split(":")
      config = HBaseConfiguration.create()
      config.set(HConstants.ZOOKEEPER_QUORUM, ipPortAry(0))
      config.set(HConstants.ZOOKEEPER_CLIENT_PORT, ipPortAry(1))
      config.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 200000)
      config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 200000)
      config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
      hBaseConfig.getScalaConfigMap().foreach {
        case (key, value) => {
          config.set(key, String.valueOf(value))
        }
      }
      connection = ConnectionFactory.createConnection(config)
      admin = connection.getAdmin.asInstanceOf[HBaseAdmin]
    } catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
        throw e
      }
    }
  }

  def existsDatabase(database: String): Boolean = {
    var exists = false
    try {
      val descriptor: NamespaceDescriptor = admin.getNamespaceDescriptor(database)
      if (descriptor != null) {
        exists = true
      }
      logger.info(s"exists database[$database]: $exists")
    }
    catch {
      case ex: NamespaceNotFoundException => {
        // ignore
      }
      case e: Exception => {
        logger.error(e.getMessage, e)
      }
    }
    exists
  }

  def existsTable(database: String, table: String): Boolean = {
    var exists = false
    try {
      exists = admin.tableExists(TableName.valueOf(database, table))
      logger.info(s"exists table[$database/$table]: $exists")
    }
    catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
      }
    }
    exists
  }

  def createDatabase(database: String): Boolean = {
    var success = false
    try {
      val namespaceDescriptor: NamespaceDescriptor = NamespaceDescriptor.create(database).build
      admin.createNamespace(namespaceDescriptor)
      success = true
      logger.info(s"Success to create database[$database]")
    }
    catch {
      case e: Exception => {
        logger.error(s"Failed to create database[$database]\n", e)
      }
    }
    success
  }

  def createTable(hbaseConfig: StoreHBaseConfig, preBuildRegion: Boolean): Boolean = {
    this.createTable(hbaseConfig, new util.ArrayList[String]() {
      add(DEFAULT_FAMILY)
    }, preBuildRegion)
  }

  def createTableIfNecessary(hbaseConfig: StoreHBaseConfig): Unit = {
    try {
      if (!existsDatabase(hbaseConfig.namespace)) {
        createDatabase(hbaseConfig.namespace)
        createTable(hbaseConfig, true)
      } else if (!existsTable(hbaseConfig.namespace, hbaseConfig.table)) {
        createTable(hbaseConfig, true)
      }
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  def getTable(tableName: String): Table = {
    connection.getTable(TableName.valueOf(tableName))
  }

  def dropTable(tableName: String): Unit = {
    dropTable(DEFAULT, tableName)
  }

  def dropTable(namespace: String, tableName: String): Unit = {
    val isExists: Boolean = existsTable(namespace, tableName)
    try {
      if (isExists) {
        admin.disableTable(TableName.valueOf(namespace, tableName))
        admin.deleteTable(TableName.valueOf(namespace, tableName))
        logger.info(s"success to drop table[$namespace:$tableName]")
      } else {
        logger.info(s"table[$namespace:$tableName] not exists")
      }
    } catch {
      case e: IOException =>
        logger.info(s"drop table[$namespace:$tableName] fail")
    }
  }

  def dropNamespace(namespace: String): Unit = {
    val isExists: Boolean = existsDatabase(namespace)
    try {
      if (isExists) {
        admin.deleteNamespace(namespace)
        logger.info(s"success to drop database[$namespace]")
      } else {
        logger.info(s"namespace [$namespace] not exists")
      }
    } catch {
      case e: IOException =>
        logger.info(s"drop namespace [$namespace] fail")
    }
  }

  def update(tableName: String, put: Put): Unit = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    table.put(put)
    table.close()
  }

  def scan(tableName: String, cols: Array[String]): List[JsonObject] = {
    val scan = new Scan()
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    if (cols != null) {
      cols.foreach(col => {
        scan.addColumn(Bytes.toBytes(col.split(":")(0)), Bytes.toBytes(col.split(":")(1)))
      })
    }
    val resultScanner: ResultScanner = table.getScanner(scan)
    var result: Result = resultScanner.next()
    var resultList: List[JsonObject] = List[JsonObject]()
    while (result != null) {
      resultList :+= resultToJSON(result)
      result = resultScanner.next()
    }
    table.close()
    resultList
  }

  def scan(tableName: String, cols: Array[String], limit: Int): List[JsonObject] = {
    val scan = new Scan()
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    if (cols != null) {
      cols.foreach(col => {
        scan.addColumn(Bytes.toBytes(col.split(":")(0)), Bytes.toBytes(col.split(":")(1)))
      })
    }
    val resultScanner: ResultScanner = table.getScanner(scan)
    var result: Result = resultScanner.next()
    var resultList: List[JsonObject] = List[JsonObject]()
    var counter = 0
    while (result != null && counter.compareTo(limit) <= 0) {
      resultList :+= resultToJSON(result)
      result = resultScanner.next()
      counter = counter + 1
    }
    table.close()
    resultList
  }

  def get(tableName: String, rowKey: String): JsonObject = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    val result: Result = table.get(get)
    table.close()
    resultToJSON(result)
  }

  def getByVersion(tableName: String, rowKey: String): JsonObject = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    get.setMaxVersions();
    get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_key"));
    val result: Result = table.get(get)
    table.close()
    resultToJSON(result)
  }

  def resultToJSON(result: Result): JsonObject = {
    val resMap = new JsonObject()
    val listCell: Array[Cell] = result.rawCells()
    for (cell <- listCell) {
      resMap.add(new String(CellUtil.cloneQualifier(cell)), new JsonPrimitive(new String(CellUtil.cloneValue(cell))))
    }
    resMap
  }

  def deleteRow(tableName: String, rowkey: String): Boolean = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val delete = new Delete(rowkey.getBytes())
    table.delete(delete)
    table.close()
    true
  }

  def bulkDelete(rowkeyList: util.List[Delete], table: Table, batchSize: Int): Boolean = {
    val tmpList = new util.ArrayList[Delete]()
    var count = 0
    for (i <- 0 until rowkeyList.size()) {
      tmpList.add(rowkeyList.get(i))
      count += 1
      if (count % batchSize == 0) {
        table.delete(tmpList)
        tmpList.clear()
      }
    }
    if (tmpList.size() > 0) {
      table.delete(rowkeyList)
    }
    true
  }

  def bulkDelete(rowkeyList: util.List[Delete], tableName: String, batchSize: Int): Boolean = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    bulkDelete(rowkeyList, table, batchSize)
  }

  def bulkUpsert(rowkeyList: util.List[Put], table: Table, batchSize: Int): Boolean = {
    val tmpList = new util.ArrayList[Put]()
    var count = 0
    for (i <- 0 until rowkeyList.size()) {
      tmpList.add(rowkeyList.get(i))
      count += 1
      if (count % batchSize == 0) {
        table.put(tmpList)
        tmpList.clear()
      }
    }
    if (tmpList.size() > 0) {
      table.put(rowkeyList)
    }
    true
  }

  def bulkUpsert(rowkeyList: util.List[Put], tableName: String, batchSize: Int): Boolean = {
    val table = connection.getTable(TableName.valueOf(tableName))
    bulkUpsert(rowkeyList, table, batchSize)
  }

  def truncate(tableName: String): Unit = {
    truncate(DEFAULT, tableName)
  }

  def truncate(namespace: String, tableName: String): Unit = {
    val table: TableName = TableName.valueOf(namespace, tableName)
    admin.disableTable(table)
    admin.truncateTable(table, true)
  }


  ///////////////////////
  // private functions
  ///////////////////////
  private def createTable(hbaseConfig: StoreHBaseConfig, families: util.List[String], preBuildRegion: Boolean): Boolean = {
    var success = false
    val database: String = hbaseConfig.namespace
    val table: String = hbaseConfig.table
    try {
      val exists: Boolean = admin.tableExists(TableName.valueOf(database, table))
      if (exists) {
        logger.info(s"Table[$database:$table] is already exists.")
        return true
      }
      val tableDesc = new HTableDescriptor(TableName.valueOf(database, table))
      import scala.collection.JavaConversions._
      for (family <- families) {
        val columnDescriptor = new HColumnDescriptor(family)
        columnDescriptor.setMaxVersions(1)
        tableDesc.addFamily(columnDescriptor)
      }
      // create
      if (preBuildRegion) {
        val logicPartitions: Int = hbaseConfig.logicPartitions
        val physicsPartitions: Int = hbaseConfig.physicsPartitions
        val splitKeys: Array[Array[Byte]] = RowKeyPartitioner.getSplitKeysBytes(logicPartitions, physicsPartitions)
        admin.createTable(tableDesc, splitKeys)
      }
      else admin.createTable(tableDesc)
      success = true
      logger.info(s"Success to create table[$database:$table]")
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        logger.error(s"Failed to create table[$database/$table]\n", e)
        throw new RuntimeException("Failed to create table,StoreType => " + StoreType.HBASE)
      }
    }
    success
  }

  def close(resources: Closeable*): Unit = {
    resources.foreach(resource => {
      try {
        resource.close()
      } catch {
        case e: Exception => resource.close()
      }
    })
  }

  def shutdown(): Unit = {
    try {
      admin.close()
      connection.close()
    } catch {
      case e: Exception =>
        admin.close()
        connection.close()
    }
  }
}
