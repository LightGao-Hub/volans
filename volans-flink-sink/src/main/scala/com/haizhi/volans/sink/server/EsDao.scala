package com.haizhi.volans.sink.server

import java.io.IOException
import java.util

import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.sink.config.store.StoreEsConfig
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.{Header, HttpEntity, HttpHost}
import org.elasticsearch.client.{Response, ResponseException, RestClient}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zhuhan on 2019/8/8.
 */
class EsDao extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[EsDao])
  private val PUT = "PUT"
  private val POST = "POST"
  private val GET = "GET"
  private val HEAD = "HEAD"
  private val DELETE = "DELETE"
  var restClient: RestClient = _

  def initClient(esConfig: StoreEsConfig): Unit = {
    val url = esConfig.url
    try {
      val hosts = url.split(",")
      val httpHosts = new ArrayBuffer[HttpHost]()
      for (hostPort <- hosts) {
        val ipPort = hostPort.split(":")
        httpHosts.+=(new HttpHost(ipPort(0), ipPort(1).toInt))
      }
      restClient = RestClient.builder(httpHosts: _*).build
      logger.info(s"success to create elasticsearch rest client url[$url]")
    }
    catch {
      case e: Exception => {
        logger.error(s"failed to create elasticsearch rest client url[$url].\n", e)
        throw e
      }
    }
  }

  def existsIndex(index: String): Boolean = {
    var exists = false
    try {
      val response = restClient.performRequest(HEAD, index)
      exists = response.getStatusLine.getReasonPhrase == "OK"
      logger.info(s"exists index[$index]: $exists")
    }
    catch {
      case e: IOException => {
        logger.error(e.getMessage, e)
      }
    }
    exists
  }

  def existsType(index: String, `type`: String): Boolean = {
    var exists = false
    try {
      val response = restClient.performRequest(HEAD,
        new StringBuilder(index).append("/_mapping/").append(`type`).toString)
      exists = response.getStatusLine.getReasonPhrase == "OK"
      logger.info(s"exists type[${index}/${`type`}]: $exists")
    }
    catch {
      case e: IOException => {
        logger.error(e.getMessage, e)
      }
    }
    exists
  }

  def createIndex(esConfig: StoreEsConfig): Boolean = {
    try {
      val index = esConfig.index
      val source = JSONUtils.toJson(esConfig.setting)
      val entity = new NStringEntity(source, ContentType.APPLICATION_JSON)
      val response = restClient.performRequest(PUT, index,
        new util.HashMap[String, String](), entity, new ArrayBuffer[Header](): _*)
      if (response.getStatusLine.getStatusCode != 200) {
        logger.error(s"Failed to create index[$index], response:\n${JSONUtils.toJson(response)}")
        return false
      }
      logger.info(s"Success to create index[$index]")
      return true
    }
    catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
      }
    }
    false
  }

  def deleteIndex(index: String): Boolean = {
    try {
      val response = restClient.performRequest(DELETE, "/" + index + "?&pretty=true")
      if (response.getStatusLine.getStatusCode != 200) {
        logger.error(s"Failed to delete type[$index], response:\n${JSONUtils.toJson(response)}")
        return false
      }
      logger.info(s"Success to delete index[$index]")
      return true
    }
    catch {
      case e: Exception => {
        logger.error(s"Failed to delete index[$index]", e)
      }
    }
    false
  }

  def createType(esConfig: StoreEsConfig): Boolean = {
    try {
      val index = esConfig.index
      val `type` = esConfig.`type`
      val url = index + "/_mapping/" + `type`
      val source = JSONUtils.toJson(esConfig.mapping)
      val entity = new NStringEntity(source, ContentType.APPLICATION_JSON)
      val response = restClient.performRequest(PUT, url,
        new util.HashMap[String, String](), entity, new ArrayBuffer[Header](): _*)
      if (response.getStatusLine.getStatusCode != 200) {
        logger.error(s"Failed to create type[${index}/${`type`}], response:\n${JSONUtils.toJson(response)}")
        return false
      }
      logger.info(s"Success to create type[${index}/${`type`}]")
      return true
    }
    catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
      }
    }
    false
  }

  def deleteDocument(esConfig: StoreEsConfig, docId: String): Boolean = {
    val index = esConfig.index
    val `type` = esConfig.`type`
    var response: Response = null
    try {
      response = restClient.performRequest(DELETE, index + "/" + `type` + "/" + docId)
    } catch {
      case e: ResponseException => response = e.getResponse()
        logger.error(e.getMessage, e)
    }
    var resultCode = false
    if (response != null) {
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode == 200) {
        resultCode = true
      } else if (statusCode == 404) {
        resultCode = true
        logger.warn(s"Document [$docId] not exists")
      }
    }
    resultCode
  }

  /**
   * bulk批量操作
   *
   * @param esConfig
   * @param bulkRequest
   * @return
   */
  def bulkOperation(esConfig: StoreEsConfig, bulkRequest: String): Response = {
    var response: Response = null
    try {
      val entity: HttpEntity = new NStringEntity(bulkRequest, ContentType.APPLICATION_JSON)
      response = restClient.performRequest(POST, "_bulk", new util.HashMap[String, String](), entity, new ArrayBuffer[Header](): _*)
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
        throw e
    }
    response
  }

  def createTableIfNecessary(esConfig: StoreEsConfig): Unit = {
    if (!existsIndex(esConfig.index)) {
      createIndex(esConfig)
      createType(esConfig)
    } else if (!existsType(esConfig.index, esConfig.`type`)) {
      createType(esConfig)
    }
  }

  def shutdown(): Unit = {
    try {
      if (restClient == null) {
        return
      }
      restClient.close()
    } catch {
      case e: Exception =>
        restClient.close()
    }
  }
}
