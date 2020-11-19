package com.haizhi.volans.sink.config.store

import com.haizhi.volans.sink.utils.EncryptDecryptHelper
import org.apache.commons.lang3.StringUtils

/**
 * Create by zhoumingbing on 2020-08-06
 */
case class StoreEsConfig(url: String,
                         index: String = "test",
                         `type`: String = "test",
                         idField: String,
                         setting: java.util.Map[String, AnyRef],
                         mapping: java.util.Map[String, AnyRef],
                         encryptionType: String = "ORIGINAL",
                         nativeConfig: java.util.Map[String, AnyRef],
                         importBatchSize: Int = 3) extends StoreConfig {

  override def getGraph: String = {
    index
  }

  override def getSchema: String = {
    `type`
  }

  def esUrlCanUseDirect(): Boolean = {
    url.split(",").map(url => url.split(":").length == 2).reduce(_ && _)
  }

  def getEsHosts(): String = {
    val urlArray: Array[String] = url.split(",")
    val hosts: StringBuilder = new StringBuilder
    for (ip <- urlArray) {
      val hostAndPort: Array[String] = ip.split(":")
      hosts.append(hostAndPort(0) + ",")
    }

    hosts.toString().split(",")(0)
  }

  def getEsPort(): String = {
    url.split(":").last
  }

  def getScalaNativeConfig(decry: Boolean): Map[String, AnyRef] = {
    import scala.collection.JavaConversions._
    if (this.nativeConfig == null) {
      return Map[String, AnyRef]()
    }

    //解密
    val map: Map[String, AnyRef] = this.nativeConfig.toMap
    if (decry) {
      for (elem <- map) {
        if (StringUtils.equalsAny(elem._1, "es.net.http.auth.user", "es.net.http.auth.pass")) {
          map.put(elem._1, EncryptDecryptHelper.decrypt(String.valueOf(elem._2), this.encryptionType))
        }
      }
    }
    map
  }
}
