package com.haizhi.volans.loader.scala.config.check

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.haizhi.volans.common.flink.base.scala.exception.ErrorMessage
import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.loader.scala.config.schema.{FieldType, Keys, SchemaFieldVo}
import com.haizhi.volans.loader.scala.config.streaming.StreamingConfig
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import collection.JavaConversions._

/**
 * 脏数据校验：
 * {"from_key":"123123", "to_key":"345345", "business_status":123.1, "address":1234, "object_key":"ertreter"}
 * {"from_key":"123123","to_key":"","business_status":123.1,"address":1234,"object_key":"ertreter"}
 * {"from_key":"123123","address":1234,"object_key":"ertreter"}
 * {"from_key":"123123","to_key":"123","address":1234,"object_key":"ertreter"}
 * {"from_key":"123123", "to_key":"345345", "business_status":"", "address":1234, "object_key":"ertreter"}
 * {"from_key":"123123", "to_key":"345345", "business_status":"123123", "address":1234, "object_key":"ertreter","sadf":123,"saaaa1df":4444}
 *
 * 类型转换：
 * {"from_key":"123123", "to_key":"345345", "business_status":"123.1", "address":1234, "object_key":"[1,2,3,4]"}
 * {"from_key":"123123", "to_key":"345345", "business_status":"1qwe23.1", "address":1234, "object_key":"[1,2,3,4]"}
 */
case class CheckValueConversion(config: StreamingConfig) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[CheckValueConversion])
  //所有Schema集合
  val fields: util.Map[String, SchemaFieldVo] = config.schemaVo.fields
  val fieldSet: Set[SchemaFieldVo] = fields.values().toSet
  val nameSet: Set[String] = fieldSet.map(_.name)

  //检验脏数据, false为脏数据
  def checkValue(value: String): (String, Boolean) = {
    //将数据转换为Map
    val stringToObject: util.Map[String, Object] = JSONUtils.jsonToJavaMap(value)
    val tuple = checkKey(stringToObject, value)
    //如果key异常，直接返回脏数据
    if (!tuple._2)
      return tuple

    val object_key = stringToObject.get(s"${Keys.OBJECT_KEY}").toString
    //检查Schema所有字段及isMain
    for (field <- fieldSet) {
      //如果不存在field字段 且 isMain == Y 为异常数据
      //如果field字段的值为null或者为空字符串，且 isMain == Y 为异常数据
      if (!stringToObject.contains(field.name) && "Y".equalsIgnoreCase(field.isMain))
        return getErrorMessageJson(object_key, value, Keys.CHECK_ERROR, s"${field.name} " +
          s"不存在 且 isMain ${field.isMain}") -> false
      else if (StringUtils.isBlank(stringToObject.get(field.name).toString) && "Y".equalsIgnoreCase(field.isMain))
        return getErrorMessageJson(object_key, value, Keys.CHECK_ERROR, s"${field.name} " +
          s"字段值为空 且 isMain ${field.isMain}") -> false
    }
    logger.info(s"数据校验正确 value :$value ")
    //删除schema中不包含的字段
    val it: util.Iterator[util.Map.Entry[String, Object]] = stringToObject.entrySet.iterator
    while (it.hasNext) {
      val item: util.Map.Entry[String, Object] = it.next
      if (!nameSet.contains(item.getKey))
        it.remove()
    }
    logger.info(s"打印删除多余字段后的 value : $stringToObject")
    //类型转换
    typeConversion(stringToObject, fields)
  }

  //类型转换,false为转换异常
  def typeConversion(stringToObject: util.Map[String, Object],
                     fields: util.Map[String, SchemaFieldVo]): (String, Boolean) = {
    try {
      val it: util.Iterator[util.Map.Entry[String, Object]] = stringToObject.entrySet.iterator
      while (it.hasNext) {
        val item: util.Map.Entry[String, Object] = it.next
        val schema: SchemaFieldVo = fields.get(item.getKey)
        item.setValue(convert(item.getValue, schema))
      }
      logger.info(s"状态修改后的 json = ${JSONUtils.toJson(stringToObject)}")
      JSONUtils.toJson(stringToObject) -> true
    } catch {
      case _: Exception =>
        logger.info(s" 类型转换异常，此条为脏数据 ")
        JSONUtils.toJson(stringToObject) -> false
    }
  }

  /**
   * 类型转换函数
   */
  def convert(value: Object, fieldVo: SchemaFieldVo): Object = {
    if (StringUtils.equalsIgnoreCase(fieldVo.`type`, FieldType.STRING)) {
      return String.valueOf(value)
    } else if (StringUtils.equalsIgnoreCase(fieldVo.`type`, FieldType.DOUBLE)) {
      return java.lang.Double.valueOf(String.valueOf(value))
    } else if (StringUtils.equalsIgnoreCase(fieldVo.`type`, FieldType.BOOLEAN)) { //这里要问一下伟林有没有boolean类型
      return java.lang.Boolean.valueOf(String.valueOf(value))
    } else if (StringUtils.equalsIgnoreCase(fieldVo.`type`, FieldType.LONG)) {
      return toLong(value)
    } else if (StringUtils.equalsIgnoreCase(fieldVo.`type`, FieldType.ARRAY)) {
      return value
    }
    value
  }

  def toLong(value: Any): java.lang.Long = {
    var longV: java.lang.Long = null
    if (value != null) {
      value match {
        case str: String =>
          if (!StringUtils.equals("null", str)) {
            longV = str.toDouble.toLong
          }
        case l: Long =>
          longV = l
        case i: Int =>
          longV = i.toLong
        case date: Date =>
          longV = date.getTime
        case _ =>
          longV = String.valueOf(value).toLong
      }
    }
    longV
  }

  /**
   * 检查Key是否存在
   *
   * @return
   */
  def checkKey(stringToObject: util.Map[String, Object], value: String): (String, Boolean) = {
    if (!stringToObject.contains(s"${Keys.OBJECT_KEY}"))
      return getErrorMessageJson("null", value, Keys.CHECK_ERROR, "object_key 不存在") -> false
    if (StringUtils.isBlank(stringToObject.get(s"${Keys.OBJECT_KEY}").toString))
      return getErrorMessageJson("null", value, Keys.CHECK_ERROR, "object_key 为空") -> false
    if (StringUtils.contains(stringToObject.get(s"${Keys.OBJECT_KEY}").toString, "/"))
      return getErrorMessageJson("null", value, Keys.CHECK_ERROR, s"objectKey must not contain a slash[/]") -> false
    val object_key = stringToObject.get(s"${Keys.OBJECT_KEY}").toString

    if (config.schemaVo.isEdge) {
      if (!stringToObject.contains(s"${Keys.FROM_KEY}"))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_ERROR, "from_key 不存在") -> false
      if (StringUtils.isBlank(stringToObject.get(s"${Keys.FROM_KEY}").toString))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_ERROR, "from_key 为空") -> false
      if (StringUtils.contains(stringToObject.get(s"${Keys.FROM_KEY}").toString, "/"))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_ERROR, s"from_key must not contain a slash[/]") -> false
      if (!stringToObject.contains(s"${Keys.TO_KEY}"))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_ERROR, "to_key 不存在") -> false
      if (StringUtils.isBlank(stringToObject.get(s"${Keys.TO_KEY}").toString))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_ERROR, "to_key 为空") -> false
      if (StringUtils.contains(stringToObject.get(s"${Keys.TO_KEY}").toString, "/"))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_ERROR, s"to_key must not contain a slash[/]") -> false
    }
    (value, true)
  }

  /**
   * 获取错误数据类型，以json形式返回
   *
   * @return
   */
  private def getErrorMessageJson(objectKey: String, value: String, code: String, errorMsg: String): String = {
    JSONUtils.toJson(ErrorMessage(s"${objectKey}_${Keys.affected_store}", objectKey, Keys.taskInstanceId,
      code, errorMsg, Keys.affected_store, value, NowDate()))
  }

  /**
   * 获取当前时间
   *
   * @return
   */
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(now)
  }

}
