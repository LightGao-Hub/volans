package com.haizhi.volans.sink.config.store

import scala.collection.JavaConversions._

/**
 * Create by zhoumingbing on 2020-08-06
 */
case class StoreHBaseConfig(url: String = null,
                            namespace: String = "default",
                            table: String = "test",
                            logicPartitions: Int = 1000,
                            physicsPartitions: Int = 16,
                            importBatchSize: Int = 2000,
                            config: java.util.Map[String, Object] = null
                           ) extends StoreConfig {
  def this() {
    this(null)
  }

  def getScalaConfigMap(): Map[String, AnyRef] = {
    if(this.config == null)Map.empty[String,AnyRef]  else this.config.toMap
  }

  def getHBaseTable(): String = {
    table
  }

  def getHBaseNamespace(): String = {
    namespace
  }

  def getHBasePort(): String = {
    url.split(":")(1)
  }

  def getHBaseHost(): String = {
    url.split(":")(0)
  }

  override def getGraph: String = {
    namespace
  }

  override def getSchema: String = {
    table
  }
}
