package com.haizhi.volans.loader.scala.config.check

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.haizhi.volans.common.flink.base.scala.exception.ErrorMessage
import com.haizhi.volans.common.flink.base.scala.util.{JSONPathUtils, JSONUtils}
import com.haizhi.volans.loader.scala.config.schema.{FieldType, Keys, SchemaFieldVo}
import com.haizhi.volans.loader.scala.config.streaming.StreamingConfig
import com.jayway.jsonpath.DocumentContext
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConversions._

/**
 * 脏数据校验：
 * 正确数据：
 * {"from_key":"123123","to_key":"345345","bicycle":{"business_status":"123.1","address":1234},"object_key":"1"}
 * 缺少object_key的脏数据：
 * {"from_key":"123123","to_key":"345345","bicycle":{"business_status":"123.1","address":1234}}
 * 缺少from_key脏数据：
 * {"to_key":"345345","bicycle":{"business_status":"123.1","address":1234},"object_key":"2"}
 * 缺少address isMain = n 的正确数据：
 * {"from_key":"123123","to_key":"345345","bicycle":{"business_status":"123.1"},"object_key":"3"}
 * 缺少address 值 isMain = n 的正确数据：
 * {"from_key":"123123","to_key":"345345","bicycle":{"business_status":"123.1","address":""},"object_key":"3"}
 * 缺少business_status ismain = y 的脏数据:
 * {"from_key":"123123","to_key":"345345","bicycle":{"address":1234},"object_key":"4"}
 * 多余字段的正确数据：
 * {"s":"c","a":"b","from_key":"123123","to_key":"345345","bicycle":{"business_status":"123.1","address":1234},"object_key":"1"}
 * 校验mix模式下无operatorFields/异常模式/正确模式
 * {"from_key":"123123","to_key":"345345","bicycle":{"business_status":"123.1","address":1234},"object_key":"1"}
 * {"from_key":"123123","to_key":"345345","bicycle":{"business_status":"123.1","address":1234},"object_key":"1", "_operation":"123"}
 * {"from_key":"123123","to_key":"345345","bicycle":{"business_status":"123.1","address":1234},"object_key":"1", "_operation":"add"}
 *
 */
case class CheckValueConversion(config: StreamingConfig) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[CheckValueConversion])
  //所有Schema集合
  private val fieldList: Seq[SchemaFieldVo] = config.schemaVo.fields.toList
  private val fieldMap: Map[String, String] = fieldList.map(field => field.targetName -> field.sourceName).toMap
  private val oerationFlag: Boolean = config.schemaVo.operation.isMix

  //检验脏数据, false为脏数据
  def checkValue(value: String): (String, Boolean) = {
    //jsonPath转换数据
    val ctx: DocumentContext = JSONPathUtils.context.parse(value)
    val tuple = checkKey(ctx, value)

    //如果key异常，直接返回脏数据
    if (!tuple._2)
      return tuple

    val object_key: String = ctx.read(s"${Keys.OBJECT_KEY}")
    val valueMap = new util.HashMap[String, Object]()
    //检查Schema所有字段及isMain
    for (field <- fieldList) {
      //如果不存在field字段 且 isMain == Y 为异常数据
      //如果field字段的值为null或者为空字符串，且 isMain == Y 为异常数据
      val fieldV: Object = ctx.read(field.sourceName)
      if (fieldV == null && "Y".equalsIgnoreCase(field.isMain))
        return getErrorMessageJson(object_key, value, Keys.CHECK_DIRTY_ERROR, s"${field.sourceName} " +
          s"字段值为空 且 isMain ${field.isMain}") -> false
      if (fieldV != null)
        valueMap.put(field.sourceName, ctx.read(field.sourceName))
    }
    logger.info(s"数据校验正确 value :$value ")

    //类型转换
    typeConversion(valueMap, fieldList)
  }

  //类型转换,false为转换异常
  def typeConversion(stringToObject: util.Map[String, Object],
                     fields: Seq[SchemaFieldVo]): (String, Boolean) = {
    try {
      for (field <- fields) {
        //由于字段可能设置 isMain = N，导致数据没有此sourceName字段
        val sourceValue: Object = stringToObject.getOrDefault(field.sourceName, null)
        if (sourceValue != null) {
          //由于字段可能设置 isMain = N，所以此字段的值有可能为""或其他类型默认值，存在类型转换异常，此时数据为脏数据
          val targetValue: Object = convert(sourceValue, field)
          //将targetName，targetValue 作为k,v传给下游
          stringToObject.remove(field.sourceName)
          stringToObject.put(field.targetName, targetValue)
        }
      }
      //logger.info(s"状态修改后的 json = ${JSONUtils.toJson(stringToObject)}")
      JSONUtils.toJson(stringToObject) -> true
    } catch {
      case e: Exception =>
        logger.info(s" 类型转换异常，此条为脏数据 ${JSONUtils.toJson(stringToObject)}")
        getErrorMessageJson(stringToObject.get(this.fieldMap(Keys.OBJECT_KEY)).toString, JSONUtils.toJson(stringToObject), Keys.TYPE_ERROR,
          s" 类型转换异常 ${e.getMessage}") -> false
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
        case d: Double =>
          longV = d.toLong
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
  def checkKey(ctx: DocumentContext, value: String): (String, Boolean) = {
    val object_key: String = ctx.read(this.fieldMap(Keys.OBJECT_KEY))
    if (StringUtils.isBlank(object_key))
      return getErrorMessageJson("null", value, Keys.CHECK_DIRTY_ERROR, "object_key 不存在 或为 null") -> false
    if (StringUtils.contains(object_key, "/"))
      return getErrorMessageJson("null", value, Keys.CHECK_DIRTY_ERROR, s"objectKey must not contain a slash[/]") -> false

    if (config.schemaVo.isEdge) {
      val from: String = ctx.read(this.fieldMap(Keys.FROM_KEY))
      if (StringUtils.isBlank(from))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_DIRTY_ERROR, "from_key 不存在") -> false
      if (StringUtils.contains(from, "/"))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_DIRTY_ERROR, s"from_key must not contain a slash[/]") -> false

      val to: String = ctx.read(this.fieldMap(Keys.TO_KEY))
      if (StringUtils.isBlank(to))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_DIRTY_ERROR, "to_key 不存在") -> false
      if (StringUtils.contains(to, "/"))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_DIRTY_ERROR, s"to_key must not contain a slash[/]") -> false
    }
    //验证MIX模式下是否具备operateField字段
    if (oerationFlag) {
      val operateField: String = ctx.read(this.fieldMap(config.schemaVo.operation.operateField))
      if (StringUtils.isBlank(operateField))
        return getErrorMessageJson(s"$object_key", value, Keys.CHECK_DIRTY_ERROR, " mix model operateField 不存在") -> false
      operateField match {
        case field if "ADD".equalsIgnoreCase(field)    =>
        case field if "UPDATE".equalsIgnoreCase(field) =>
        case field if "DELETE".equalsIgnoreCase(field) =>
        case field if "UPSERT".equalsIgnoreCase(field) =>
        case _ => return getErrorMessageJson(s"$object_key", value, Keys.CHECK_DIRTY_ERROR, s" mix model operateField 类型错误 : $operateField ") -> false
      }
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
