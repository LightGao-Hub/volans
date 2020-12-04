package com.haizh.volans.sink

import com.haizhi.volans.sink.server.HiveDao

/**
 * Author pengxb
 * Date 2020/12/4
 */
object HiveTest {
  def main(args: Array[String]): Unit = {
    val hiveDao = new HiveDao()
    val table = hiveDao.getTable("bigdata_test", "p1")
    // create table p1(id bigint,name string)
    // partitioned by (k string,a tinyint,b smallint,c int,d bigint,e float,f double,g binary,h boolean,i char(4),j varchar(20));

    val partValueList = List("China",1,2.toShort,3,5l,1.5f,2.35d,Array(3.toByte),true,"3","33")
//    val filterExpr = HiveUtils.getPartitionFilterExpr(table,partValueList.asJava)
//    println(filterExpr)
    hiveDao.shutdown()
  }
}
