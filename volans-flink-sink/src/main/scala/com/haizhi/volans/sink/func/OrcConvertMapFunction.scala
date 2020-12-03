package com.haizhi.volans.sink.func

import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.sink.util.OrcUtils
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.data.{GenericRowData, RowData}

/**
 * Author pengxb
 * Date 2020/12/2
 */
class OrcConvertMapFunction(var fieldSchemaList: List[(String, String)])
  extends MapFunction[String, RowData] {

  override def map(value: String): RowData = {
    val valueMap = JSONUtils.jsonToJavaMap(value)
    val rowData = new GenericRowData(fieldSchemaList.size)
    for (i <- 0 until fieldSchemaList.size) {
      val fieldName = fieldSchemaList(i)._1
      val fieldType = fieldSchemaList(i)._2
      rowData.setField(i, OrcUtils.generateRowDataValue(valueMap.get(fieldName), fieldType))
    }
    rowData
  }

}
