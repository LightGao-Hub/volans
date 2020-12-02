package com.haizhi.jsonpath
import java.util
import java.util.Date

import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.jayway.jsonpath._
import com.jayway.jsonpath.internal.JsonContext
import com.jayway.jsonpath.Configuration
import org.apache.commons.lang3.StringUtils

object TestPath {

  def main(args: Array[String]): Unit = {

    val json = "{\n\t\"store\": {\n\t\t\"bicycle\": {\n\t\t\t\"color\": \"red\",\n\t\t\t\"price\": 19.95\n\t\t}\n\t},\n\t\"expensive\": \"8\",\n\t\"arr\": [1, 2, 3],\n\t\"aa\": 123,\n\t\"bb\": \"\"\n}"

    val conf = Configuration.defaultConfiguration.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)

    val context = JsonPath.using(conf)

    val ctx: DocumentContext = context.parse(json)

    val value2:Object = ctx.read("$.store.bicycle.price") //double

    println(value2)

    val value5:Object = ctx.read("$.arr") //数组

    println(value5)

    val value6:Object = ctx.read("$.aa") //long

    println(value6)

    val value7:Object = ctx.read("$.expensive") //string

    println(value7)

    val doublev: Object = convert(value6, "double")
    val longv: Object = convert(value2, "long")
    val arrayv: Object = convert(value5, "array")
    val slongv: Object = convert(value7, "array")

    println(" long 转换 double " + doublev)
    println(" double 转换 long " + longv)
    println(" array 转换 array " + arrayv)
    println(" string 转换 long " + slongv)

    val objectToV = new util.HashMap[String, Object]()
    objectToV.put("a", doublev)
    objectToV.put("b", longv)
    objectToV.put("c", arrayv)
    objectToV.put("d", slongv)

    val str = JSONUtils.toJson(objectToV)
    println(s"转换后的json $str")

    val value3:String = ctx.read("$.store.bicycle.price2") //没有

    println(StringUtils.isBlank(value3))


  }

  /**
   * 类型转换函数
   */
  def convert(value: Object, fieldVo: String): Object = {
    if (StringUtils.equalsIgnoreCase(fieldVo, FieldType.STRING)) {
      return String.valueOf(value)
    } else if (StringUtils.equalsIgnoreCase(fieldVo , FieldType.DOUBLE)) {
      return java.lang.Double.valueOf(String.valueOf(value))
    } else if (StringUtils.equalsIgnoreCase(fieldVo , FieldType.BOOLEAN)) { //这里要问一下伟林有没有boolean类型
      return java.lang.Boolean.valueOf(String.valueOf(value))
    } else if (StringUtils.equalsIgnoreCase(fieldVo , FieldType.LONG)) {
      return toLong(value)
    } else if (StringUtils.equalsIgnoreCase(fieldVo , FieldType.ARRAY)) {
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

}
