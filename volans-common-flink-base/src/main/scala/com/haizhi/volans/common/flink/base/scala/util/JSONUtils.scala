package com.haizhi.volans.common.flink.base.scala.util

import java.lang.reflect.Type
import java.util

import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, GsonBuilder, JsonObject}
import com.haizhi.volans.common.flink.base.java.util.MapTypeAdapter

import scala.collection.{JavaConverters, mutable}

/**
  * Create by zhoumingbing on 2020-08-06
  */
object JSONUtils {
  //private val gson: Gson = new Gson();
  val gson: Gson = new GsonBuilder().registerTypeAdapter(new TypeToken[util.Map[String, AnyRef]]() {}.getType, new MapTypeAdapter).create

  def toMap[T](bean: T): util.Map[String, AnyRef] = {
    jsonToMap(toJson(bean))
  }

  def toJsonObject(json: String): JsonObject = {
    gson.fromJson(json, classOf[JsonObject])
  }

  def toJson[T](bean: T): String = {
    gson.toJson(bean)
  }

  def jsonToMap(json: String): util.Map[String, AnyRef] = {
    gson.fromJson(json, new TypeToken[util.Map[String, AnyRef]]() {}.getType)
  }

  def fromJson[T](json: String, classOfT: Class[T]): T={
    gson.fromJson(json,classOfT)
  }

  def fromJson[T](json: String, typeOfT: Type): T = {
    gson.fromJson(json, typeOfT)
  }

  def toListMap[T](beanList: util.List[T]): util.List[util.Map[String, AnyRef]] = {
    jsonToListMap(toJson(beanList))
  }

  def jsonToListMap(json: String): util.List[util.Map[String, AnyRef]] = {
    gson.fromJson(json, new TypeToken[util.List[util.Map[String, AnyRef]]]() {}.getType)
  }

  def toListMap(obj: Any): util.List[util.Map[String, AnyRef]] = {
    jsonToListMap(toJson(obj))
  }

  def toJavaListMap(obj: Any): java.util.List[java.util.Map[String, AnyRef]] = {
    gson.fromJson(toJson(obj), new TypeToken[util.List[util.Map[String, AnyRef]]]() {}.getType)
  }

  def jsonToJavaMap(json: String): util.Map[String, Object] = {
    gson.fromJson(json, new TypeToken[util.Map[String, Object]]() {}.getType)
  }

  def jsonToScalaMap(json: String): mutable.Map[String, AnyRef] = {
    val map: util.Map[String, AnyRef] = jsonToMap(json)
    val scala: mutable.Map[String, AnyRef] = JavaConverters.mapAsScalaMapConverter(map).asScala
    scala
  }

  def main(args: Array[String]): Unit = {
    val value: String = "{\"source\":{\"hiveSQL\":\"SELECT `_key`,address,business_status,capital,city,contact,ctime,enterprise_type,industry,industry_label,is_listed,name,object_key,operation_startdate,province,registered_address,registered_capital_unit,unified_social_credit_code,utime FROM benchmark.company\"},\"sinks\":[{\"storeType\":\"ES\",\"storeConfig\":{\"index\":\"benchmark.company\",\"mapping\":{\"dynamic_date_formats\":[\"yyyy-MM-dd HH:mm:ss\",\"yyyy-MM-dd\"],\"dynamic_templates\":[{\"strings\":{\"mapping\":{\"analyzer\":\"ik\",\"type\":\"text\",\"fields\":{\"keyword\":{\"normalizer\":\"my_normalizer\",\"type\":\"keyword\"}}},\"match_mapping_type\":\"string\"}}],\"_all\":{\"enabled\":false},\"date_detection\":true},\"setting\":{\"analysis\":{\"normalizer\":{\"my_normalizer\":{\"filter\":[\"lowercase\",\"asciifolding\"],\"char_filter\":[],\"type\":\"custom\"}},\"analyzer\":{\"ik\":{\"type\":\"custom\",\"tokenizer\":\"ik_max_word\"}}},\"index.number_of_replicas\":1,\"index.number_of_shards\":5},\"type\":\"company\",\"url\":\"192.168.2.71:9200\"}}],\"logSink\":null,\"gapConfig\":{\"errorMode\":-1,\"errorStoreEnabled\":true,\"errorStoreRowsLimit\":0,\"fieldMapping\":{},\"inboundTaskId\":\"flow_hqMWmq4FZ-GDB\",\"schema\":{\"fields\":{\"_key\":{\"id\":-15003,\"isMain\":\"N\",\"name\":\"_key\",\"required\":\"N\",\"type\":\"STRING\"},\"address\":{\"id\":-15001,\"isMain\":\"Y\",\"name\":\"address\",\"required\":\"Y\",\"type\":\"STRING\"},\"business_status\":{\"id\":-15006,\"isMain\":\"N\",\"name\":\"business_status\",\"required\":\"N\",\"type\":\"STRING\"},\"capital\":{\"id\":-15007,\"isMain\":\"N\",\"name\":\"capital\",\"required\":\"N\",\"type\":\"DOUBLE\"},\"city\":{\"id\":-15005,\"isMain\":\"N\",\"name\":\"city\",\"required\":\"N\",\"type\":\"STRING\"},\"contact\":{\"id\":-15004,\"isMain\":\"N\",\"name\":\"contact\",\"required\":\"N\",\"type\":\"STRING\"},\"ctime\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"ctime\",\"required\":\"N\",\"type\":\"STRING\"},\"enterprise_type\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"enterprise_type\",\"required\":\"N\",\"type\":\"STRING\"},\"industry\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"industry\",\"required\":\"N\",\"type\":\"STRING\"},\"industry_label\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"industry_label\",\"required\":\"N\",\"type\":\"STRING\"},\"is_listed\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"is_listed\",\"required\":\"N\",\"type\":\"STRING\"},\"name\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"name\",\"required\":\"N\",\"type\":\"STRING\"},\"object_key\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"object_key\",\"required\":\"N\",\"type\":\"STRING\"},\"operation_startdate\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"operation_startdate\",\"required\":\"N\",\"type\":\"STRING\"},\"province\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"province\",\"required\":\"N\",\"type\":\"STRING\"},\"registered_address\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"registered_address\",\"required\":\"N\",\"type\":\"STRING\"},\"registered_capital_unit\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"registered_capital_unit\",\"required\":\"N\",\"type\":\"STRING\"},\"unified_social_credit_code\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"unified_social_credit_code\",\"required\":\"N\",\"type\":\"STRING\"},\"utime\":{\"id\":-15002,\"isMain\":\"N\",\"name\":\"utime\",\"required\":\"N\",\"type\":\"STRING\"}},\"graphId\":-11100,\"id\":-15000,\"name\":\"eg_upstream\",\"type\":\"edge\",\"useGdb\":\"Y\",\"useHBase\":\"N\",\"useSearch\":\"Y\"},\"taskInstanceId\":\"flow_hqMWmq4FZ\"},\"sparkConfig\":{\"numPartitions\":24}}"
    val map: mutable.Map[String, AnyRef] = jsonToScalaMap(value)
    println(map)
  }
}
