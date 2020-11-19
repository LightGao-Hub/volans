package com.haizhi.volans.sink.config.constant

/**
  * Created by zhuhan on 2019/8/8.
  */
object FieldType {
  val STRING = "STRING"
  val DOUBLE = "DOUBLE"
  val LONG = "LONG"
  val DATETIME = "DATETIME"
  val ARRAY = "ARRAY"
  val OBJECT_ARRAY = "OBJECT_ARRAY"
  val OBJECT = "OBJECT"

  def getJavaFieldType(fieldType: String): JavaFieldType = {
    fieldType match {
      case STRING => JavaFieldType.STRING
      case DOUBLE => JavaFieldType.DOUBLE
      case LONG => JavaFieldType.LONG
      case DATETIME => JavaFieldType.DATETIME
      case ARRAY => JavaFieldType.ARRAY
      case _ => JavaFieldType.UNKNOWN
    }
  }
}
