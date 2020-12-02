package com.haizhi.volans.sink.util

import com.haizhi.volans.sink.config.constant.HiveFieldType
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.{RowData, StringData}
import org.apache.flink.table.types.logical.LogicalType

/**
 * Author pengxb
 * Date 2020/12/2
 */
object OrcUtils {

  val PATTERN_VARCHAR = """varchar\((\d+)\)""".r
  val PATTERN_CHAR = """char\((\d+)\)""".r
  val PATTERN_BINARY = """binary\((\d+)\)""".r

  /**
   * Hive字段转LogicalType
   *
   * @param fieldSchemaList
   * @return
   */
  def convertHiveFieldToLogicalType(fieldSchemaList: List[(String, String)]): Array[LogicalType] = {
    val logicalTypes: Array[LogicalType] = new Array[LogicalType](fieldSchemaList.size)
    for (i <- 0 until fieldSchemaList.size) {
      val fieldType = fieldSchemaList(i)._2
      logicalTypes(i) = fieldType.toLowerCase match {
        case HiveFieldType.TINYINT => DataTypes.TINYINT().getLogicalType
        case HiveFieldType.SMALLINT => DataTypes.SMALLINT().getLogicalType
        case HiveFieldType.INT => DataTypes.INT().getLogicalType
        case HiveFieldType.BIGINT => DataTypes.BIGINT().getLogicalType
        case HiveFieldType.FLOAT => DataTypes.FLOAT().getLogicalType
        case HiveFieldType.DOUBLE => DataTypes.DOUBLE().getLogicalType
        case HiveFieldType.BOOLEAN => DataTypes.BOOLEAN().getLogicalType
        case HiveFieldType.BINARY => DataTypes.BYTES().getLogicalType
        case HiveFieldType.STRING => DataTypes.STRING().getLogicalType
        case PATTERN_VARCHAR(n) => DataTypes.VARCHAR(n.toInt).getLogicalType
        case PATTERN_CHAR(n) => DataTypes.CHAR(n.toInt).getLogicalType
        // ToDo
        // case HiveFieldType.MAP => DataTypes.MAP(null,null).getLogicalType
        // case HiveFieldType.ARRAY => DataTypes.ARRAY(null).getLogicalType
        // case HiveFieldType.STRUCT => DataTypes.ROW(null).getLogicalType
        case _ => throw new Exception(s"不支持的Hive数据类型: $fieldType")
      }
    }
    logicalTypes
  }

  /**
   * 设置RowData值
   *
   * @param value
   * @param fieldType
   * @return
   */
  def generateRowDataValue(value: Object, fieldType: String): Any = {
    fieldType.toLowerCase() match {
      case HiveFieldType.TINYINT => value.asInstanceOf[Byte]
      case HiveFieldType.SMALLINT => value.asInstanceOf[Short]
      case HiveFieldType.INT => value.asInstanceOf[Int]
      case HiveFieldType.BIGINT => value.asInstanceOf[Long]
      case HiveFieldType.FLOAT => value.asInstanceOf[Float]
      case HiveFieldType.DOUBLE => value.asInstanceOf[Double]
      case HiveFieldType.BOOLEAN => value.asInstanceOf[Boolean]
      case HiveFieldType.STRING | PATTERN_VARCHAR() | PATTERN_CHAR() => StringData.fromString(value.toString)
      case HiveFieldType.BINARY => value.asInstanceOf[Array[Byte]]
      case _ => StringData.fromString(value.toString)
    }
  }

  /**
   * 获取RowData值
   * @param value
   * @param fieldType
   * @param location
   * @return
   */
  def getRowDataValue(value: RowData, fieldType: String, location: Int): Any = {
    fieldType.toLowerCase() match {
      case HiveFieldType.TINYINT => value.getByte(location)
      case HiveFieldType.SMALLINT => value.getShort(location)
      case HiveFieldType.INT => value.getInt(location)
      case HiveFieldType.BIGINT => value.getLong(location)
      case HiveFieldType.FLOAT => value.getFloat(location)
      case HiveFieldType.DOUBLE => value.getDouble(location)
      case HiveFieldType.BOOLEAN => value.getBoolean(location)
      case HiveFieldType.STRING | PATTERN_VARCHAR() | PATTERN_CHAR() => value.getString(location)
      case HiveFieldType.BINARY => value.getBinary(location)
      case _ => value.getString(location)
    }
  }
}
