//package com.haizh.volans.sink
//
//import java.util
//
//import com.google.gson.Gson
//import com.google.gson.reflect.TypeToken
//import org.apache.flink.api.common.functions.RuntimeContext
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
//import org.apache.flink.util.ExceptionUtils
//import org.apache.http.impl.client.BasicCredentialsProvider
//import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
//import org.apache.http.message.BasicHeader
//import org.apache.http.{Header, HttpHost}
//import org.elasticsearch.ElasticsearchParseException
//import org.elasticsearch.action.ActionRequest
//import org.elasticsearch.client.{Requests, RestClientBuilder}
//import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
//import org.elasticsearch.common.xcontent.XContentType
//import org.slf4j.LoggerFactory
//
///**
// * Author pengxb
// * Date 2020/11/16
// */
//object StreamToEsVer6 {
//
//  val logger = LoggerFactory.getLogger(getClass)
//
//  def main(args: Array[String]): Unit = {
//    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    senv.enableCheckpointing(10000)
//
//    /**
//     * 样例数据：
//     * {"name":"李四","age":22,"gender":"male","country":"China","province":"Guangzhou","id":"1","_operation":"INSERT"}
//     * {"name":"赵六","age":26,"gender":"male","country":"China","province":"Shanghai","id":"2","_operation":"INSERT"}
//     *
//     * {"name":"李四","age":22,"gender":"male","country":"China","province":"Changsha","id":"1","_operation":"UPSERT"}
//     * {"name":"赵六","age":26,"gender":"male","country":"China","province":"Shanghai","id":"2","_operation":"DELETE"}
//     */
//    val input: DataStream[Iterable[String]] = senv
//      .socketTextStream("localhost", 9999)
//      .map(_ -> 1)
//      .keyBy(_._2)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//      .process(new MyProcessWindowFunction)
//
//
//
////    input.addSink(esSinkBuilder.build)
//    senv.execute("Flink-ElasticSearch Test")
//  }
//
//}
