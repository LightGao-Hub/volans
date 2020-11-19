package com.haizhi.volans.sink.config

import com.haizhi.volans.sink.utils.EncryptDecryptHelper

/**
  * Create by zhoumingbing on 2020-08-06
  */
case class StoreAtlasConfig(url: String = null,
                            database: String = "_system",
                            collection: String = "Vertex",
                            collectionType: String = "vertex",
                            encryptionType: String = "ORIGINAL",
                            user: String = null,
                            password: String = null,
                            maxConnections: Int = 1,
                            numberOfShards: Int = 9,
                            replicationFactor: Int = 1,
                            importBatchSize: Int = 2000
                           ) extends StoreConfig {
  def this() {
    this(null)
  }

  override def getGraph: String = {
    database
  }

  override def getSchema: String = {
    collection
  }

  def isVertex(): Boolean = {
    "vertex".equals(this.collectionType.toLowerCase)
  }

  def isEdge(): Boolean = {
    !isVertex()
  }

  def getUser(encrypt: Boolean): String = {
    if (!encrypt) {
      return this.user
    }
    EncryptDecryptHelper.decrypt(this.user, this.encryptionType)
  }

  def getPassword(encrypt: Boolean): String = {
    if (!encrypt) {
      return this.password
    }
    EncryptDecryptHelper.decrypt(this.password, this.encryptionType)
  }

}
