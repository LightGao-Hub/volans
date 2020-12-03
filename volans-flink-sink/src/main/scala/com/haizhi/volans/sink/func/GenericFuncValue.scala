package com.haizhi.volans.sink.func

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.table.data.{GenericRowData, RowData}

/**
 * Author pengxb
 * Date 2020/11/24
 *
 * 泛型方法参数默认值
 */
object GenericFuncValue {

  /**
   * Hive Sink使用
   */
  val GENERICRECORD: GenericRecord = new GenericRecord() {
    override def put(key: String, v: Any): Unit = ???

    override def get(key: String): AnyRef = ???

    override def put(i: Int, v: Any): Unit = ???

    override def get(i: Int): AnyRef = ???

    override def getSchema: Schema = ???
  }


  val ITERABLE_STRING: Iterable[String] = Iterable("")

  val GENERICROWDATA: RowData = new GenericRowData(1)

}
