package com.haizhi.volans.sink.util

/**
  * @author Hanson
  * @create 2019-07-31
  */
object LocalFileUtils {

  def readFile2String(path: String): String = {
    //导入Scala的IO包
    import scala.io.Source
    //以指定的UTF-8字符集读取文件，第一个参数可以是字符串或者是java.io.File
    val source = Source.fromFile(path, "UTF-8")
    //或取文件中所有行
    val lines = source.getLines()
    val sb = new StringBuilder()
    lines.foreach(line => {
      sb.append(line)
    })
    //将所有行放到数组中
    source.close()
    sb.toString()
  }

  def usingWithClose[A <: {def close():Unit}, B](resource:A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }
}
