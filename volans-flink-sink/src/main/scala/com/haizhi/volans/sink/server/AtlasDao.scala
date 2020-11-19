package com.haizhi.volans.sink.server

import java.util
import java.util.Objects

import com.arangodb.entity.{CollectionType, IndexEntity, LoadBalancingStrategy}
import com.arangodb.model._
import com.arangodb.{ArangoCollection, ArangoDB, ArangoDBException, ArangoDatabase}
import com.haizhi.volans.sink.config.store.StoreAtlasConfig
import com.haizhi.volans.sink.config.constant.IndexTypeEnum.IndexTypeEnum
import com.haizhi.volans.sink.config.constant.{IndexTypeEnum, Keys}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
 * Created by zhuhan on 2019/8/8.
 */
class AtlasDao extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  private var arangoDB: ArangoDB = _

  def getArangoDB(): ArangoDB = {
    arangoDB
  }

  def initClient(atlasConfig: StoreAtlasConfig): Unit = {
    try {
      val url = atlasConfig.url
      val builder = new ArangoDB.Builder
      val hostArray = url.split(",")
      for (host <- hostArray) {
        val ipPort = host.split(":")
        builder.host(ipPort(0), ipPort(1).toInt)
      }
      arangoDB = builder.user(atlasConfig.getUser(true)).password(atlasConfig.getPassword(true))
        .timeout(10000)
        .loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN)
        //.maxConnections(atlasConfig.maxConnections)
        .acquireHostList(true)
        .connectionTtl(5 * 60 * 1000L).build
      logger.info(s"success to create client of atlas server url[$url]")
    } catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
        throw e
      }
    }
  }

  def existsDatabase(database: String): Boolean = {
    try {
      val exists = arangoDB.getDatabases.contains(database)
      logger.info(s"exists database[$database]: $exists")
      return exists;
    }
    catch {
      case e: ArangoDBException => {
        logger.error(e.getMessage, e)
      }
    }
    false
  }

  def existsTable(database: String, table: String): Boolean = {
    try {
      var exists = false
      val adb = arangoDB.db(database)
      import scala.collection.JavaConversions._
      for (collectionEntity <- adb.getCollections) {
        if (collectionEntity.getName == table) {
          exists = true
        }
      }
      logger.info(s"exists table[$database/$table]: $exists")
      return exists
    }
    catch {
      case e: ArangoDBException => {
        logger.error(e.getMessage, e)
      }
    }
    false
  }

  def createDatabase(database: String): Boolean = {
    try {
      val success = arangoDB.createDatabase(database)
      logger.info(s"create database [$database], success=$success")
      true
    }
    catch {
      case e: ArangoDBException => {
        logger.error(e.getMessage, e)
        false
      }
    }
  }

  def createTable(atlasConfig: StoreAtlasConfig): Boolean = {
    try {
      val database = atlasConfig.database
      val table = atlasConfig.collection
      val options = new CollectionCreateOptions
      options.numberOfShards(atlasConfig.numberOfShards)
      options.replicationFactor(atlasConfig.replicationFactor)
      options.shardKeys(Keys._KEY)
      if (atlasConfig.isVertex()) {
        options.`type`(CollectionType.DOCUMENT)
      }
      else {
        options.`type`(CollectionType.EDGES)
      }
      // create table
      val adb = arangoDB.db(database)
      val resEntity = adb.createCollection(table, options)
      if (StringUtils.isBlank(resEntity.getId)) {
        logger.error(s"Failed to create table[$database/$table]")
        return false
      }
      // create index
      createIndex(adb, database, table, IndexTypeEnum.HASHINDEX, util.Arrays.asList(Keys._KEY), new HashIndexOptions)
      logger.info(s"success to create table[$database/$table]")
      true
    }
    catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
        false
      }
    }
  }

  def dropTable(atlasConfig: StoreAtlasConfig): Boolean = {
    try {
      arangoDB.db(atlasConfig.database).collection(atlasConfig.collection).drop()
      logger.info(s"success to drop table [$atlasConfig.database/$atlasConfig.collection]")
      true
    } catch {
      case e: Exception =>
        logger.info(s"drop table [$atlasConfig.database/$atlasConfig.collection] fail")
        false
    }
  }

  def dropDatabase(atlasConfig: StoreAtlasConfig): Boolean = {
    try {
      arangoDB.db(atlasConfig.database).drop()
      logger.info(s"success to drop database [$atlasConfig.database]")
      true
    } catch {
      case e: Exception =>
        logger.info(s"drop table [$atlasConfig.database] fail")
        false
    }
  }

  def shutdown(): Unit = {
    try {
      arangoDB.shutdown()
    } catch {
      case e: Exception =>
        arangoDB.shutdown()
    }
  }

  ///////////////////////
  // private functions
  ///////////////////////
  private def createIndex(adb: ArangoDatabase, database: String, table: String, indexTypeEnum: IndexTypeEnum, fields: util.Collection[String], options: Any) = {
    var indexEntity: IndexEntity = null
    val arangoCollection = adb.collection(table)
    if (Objects.isNull(indexTypeEnum)) {
      logger.error("indexTypeEnum should not be null!")
      indexEntity
    }
    try
      indexTypeEnum match {
        case IndexTypeEnum.HASHINDEX =>
          indexEntity = arangoCollection.ensureHashIndex(fields, options.asInstanceOf[HashIndexOptions])
        case IndexTypeEnum.SKIPLISTINDEX =>
          indexEntity = arangoCollection.ensureSkiplistIndex(fields, options.asInstanceOf[SkiplistIndexOptions])
        case IndexTypeEnum.FULLTEXTINDEX =>
          indexEntity = arangoCollection.ensureFulltextIndex(fields, options.asInstanceOf[FulltextIndexOptions])
        case IndexTypeEnum.GEOINDEX =>
          indexEntity = arangoCollection.ensureGeoIndex(fields, options.asInstanceOf[GeoIndexOptions])
        case IndexTypeEnum.PERSISTENTINDEX =>
          indexEntity = arangoCollection.ensurePersistentIndex(fields, options.asInstanceOf[PersistentIndexOptions])
        case _ =>
          logger.error(s"indexTypeEnum:[$indexTypeEnum] is not type of IndexTypeEnum!")
      }
    catch {
      case e: ArangoDBException => {
        logger.error(s"create index error on table[${database}/${table}\n",e)
      }
    }
    indexEntity
  }

  def createTableIfNecessary(atlasConfig: StoreAtlasConfig): Unit = {
    try {
      if (!existsDatabase(atlasConfig.database)) {
        createDatabase(atlasConfig.database)
        createTable(atlasConfig)
      } else if (!existsTable(atlasConfig.database, atlasConfig.collection)) {
        createTable(atlasConfig)
      }
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        shutdown()
        sys.exit(1)
    } finally {
      shutdown()
    }
  }

  def updateArango(elements: List[String], col: ArangoCollection, op: DocumentImportOptions, batchSize: Int): Unit = {
    var i = 1
    val tmpList = new util.ArrayList[String]()
    elements.foreach(element => {
      tmpList.add(element)
      if (i % batchSize == 0) {
        col.importDocuments(tmpList, op)
        tmpList.clear()
      }
      i = i + 1
    })
    col.importDocuments(tmpList, op)
  }

  def deleteArango(elements: List[String], col: ArangoCollection, batchSize: Int): Unit = {
    var i = 1
    val tmpList = new util.ArrayList[String]()
    elements.foreach(element => {
      tmpList.add(element)
      if (i % batchSize == 0) {
        col.deleteDocuments(tmpList)
        tmpList.clear()
      }
      i = i + 1
    })
    col.deleteDocuments(tmpList)
  }

}
