package com.haizhi.volans.sink.component

import java.util

import com.google.gson.Gson
import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.sink.config.store.StoreEsConfig
import com.haizhi.volans.sink.config.constant.{CoreConstants, Keys, StoreType}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.flink.util.ExceptionUtils
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.http.{Header, HttpHost}
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

/**
 * Author pengxb
 * Date 2020/11/18
 *
  */
class EsSink(override var storeType: StoreType, var storeConfig: StoreEsConfig)
  extends Sink with Serializable {
  /**
   * 需要实现序列化接口，否则会报错
   * The implementation of the provided ElasticsearchSinkFunction is not serializable. The object probably contains or references non-serializable fields
   */

  private val logger = LoggerFactory.getLogger(classOf[EsSink])

  /**
   * 构建ES Sink
   * @return
   */
  def buildEsSink: RichSinkFunction[Iterable[String]] = {
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost(storeConfig.getEsHosts(), storeConfig.getEsPort().toInt, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[Iterable[String]](
      httpHosts,
      new ElasticsearchSinkFunction[Iterable[String]] {
        def process(elements: Iterable[String], ctx: RuntimeContext, indexer: RequestIndexer) {
          logger.debug("Elements: " + elements)
          elements.foreach(record => {
            val element = JSONUtils.fromJson(record, classOf[util.Map[String, Object]])
            validateAndMerge(element)
            val operation = element.get(Keys._OPERATION)
            if (operation != null) {
              element.remove(Keys._OPERATION)
            }
            // Delete Operation
            if (operation != null && CoreConstants.OPERATION_DELETE.equalsIgnoreCase(operation.toString)) {
              indexer.add(
                Requests.deleteRequest(storeConfig.index)
                  .`type`(storeConfig.`type`)
                  .id(element.get(Keys.ID).toString)
              )
            } else {
              // Upsert Operation
              indexer.add(
                Requests.indexRequest
                  .index(storeConfig.index)
                  .`type`(storeConfig.`type`)
                  .id(element.get(Keys.ID).toString)
                  .source(JSONUtils.toJson(element), XContentType.JSON)
              )
            }
          })
        }
      }
    )

    // 重新处理请求失败的记录
    esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler {
      @throws(classOf[Throwable])
      override def onFailure(action: ActionRequest, failure: Throwable, restStatusCode: Int, indexer: RequestIndexer) {
        if (ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]).isPresent()) {
          // full queue; re-add document for indexing
          indexer.add(action)
        } else if (ExceptionUtils.findThrowable(failure, classOf[ElasticsearchParseException]).isPresent()) {
          // malformed document; simply drop request without failing sink
        } else {
          // for all other failures, fail the sink
          // here the failure is simply rethrown, but users can also choose to throw custom exceptions
          throw failure
        }
      }
    })
    // 设置批量操作数
    esSinkBuilder.setBulkFlushMaxActions(3)
    // 根据自定义参数创建RestClientFactory
    esSinkBuilder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
        restClientBuilder.setDefaultHeaders(Array[Header](new BasicHeader("Content-Type", "application/json")))
        restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
          override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = { // elastic search username and password
            val credentialsProvider = new BasicCredentialsProvider()
            // credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "changeme"))
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
          }
        })
      }
    })
    esSinkBuilder.build()
  }

  /**
   * 参数校验、转换
   * @param element
   */
  def validateAndMerge(element: util.Map[String, Object]): Unit = {
    if (element.containsKey(Keys.OBJECT_KEY) && !element.containsKey(Keys.ID)) {
      element.put(Keys.ID, Keys.OBJECT_KEY)
    }
  }

  override def build[T](v: T): RichSinkFunction[T] = {
    this.buildEsSink.asInstanceOf[RichSinkFunction[T]]
  }

}
