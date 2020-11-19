package com.haizhi.volans.sink.constant

/**
 * Author pengxb
 * Date 2020/11/3
 */
object HiveStoreType {

  val ORC: String = "orc"
  val TEXTFILE: String = "text"
  val PARQUET: String = "parquet"
  val RCFILE: String = "rcfile"
  val AVRO: String = "avro"

  def getStoreType(inputFormat: String): String = {
    if (inputFormat.contains("io.parquet")) {
      HiveStoreType.PARQUET
    } else if (inputFormat.contains("io.orc")) {
      HiveStoreType.ORC
    } else if (inputFormat.contains("io.rcfile")) {
      HiveStoreType.RCFILE
    } else if (inputFormat.contains("io.avro")) {
      HiveStoreType.AVRO
    }else {
      HiveStoreType.TEXTFILE
    }
  }

}
