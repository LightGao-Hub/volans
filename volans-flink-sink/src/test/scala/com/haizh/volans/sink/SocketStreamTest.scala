package com.haizhi.volans.sink


import com.haizhi.volans.sink.component.{FileHandleSink, HiveSink, SinkContext}
import com.haizhi.volans.sink.config.constant.{HiveStoreType, Keys}
import com.haizhi.volans.sink.func.{AvroConvertMapFunction, GenericFuncValue, OrcConvertMapFunction}
import com.haizhi.volans.sink.util.LocalFileUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

/**
 * Author pengxb
 * Date 2020/11/17
 */
object SocketStreamTest {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //    senv.registerType(classOf[HttpHost])
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    senv.enableCheckpointing(10000)

    /**
     * 样例数据：
     * {"name":"张三","age":22,"gender":"male","country":"China","province":"Guangzhou","object_key":"3","_operation":"INSERT"}
     * {"name":"张三","age":22,"gender":"male","country":"China","province":"Guangzhou","object_key":"3","_operation":"DELETE"}
     */

    println(s"userConfig: ${senv.getConfig.getGlobalJobParameters.toMap}")

    val hostname = if (args.length > 0) {
      args(0)
    } else {
      "localhost"
    }

    val stream: DataStream[Iterable[String]] = senv
      .socketTextStream(hostname, 9999)
      .map(_ -> "default")
      .keyBy(_._2)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .process(new ProcessWindowFunction[(String, String), Iterable[String], String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[Iterable[String]]): Unit = {
          out.collect(elements.map(_._1))
        }
      })

    var jsonStr =
      """
        |{
        |    "sinks":[
        |         {
        |            "storeType":"HBASE",
        |            "storeConfig":{
        |                "url":"192.168.1.131,192.168.1.132,192.168.1.134:2181",
        |                "namespace":"default",
        |                "table":"flink_test.person",
        |                "logicPartitions":1000,
        |                "physicsPartitions":16,
        |                "config":{
        |
        |                }
        |            }
        |        }
        |    ],
        |    "schema":{
        |        "fields":{
        |            "name":{
        |                "isMain":"N",
        |                "name":"name",
        |                "type":"STRING"
        |            },
        |            "age":{
        |                "isMain":"N",
        |                "name":"age",
        |                "type":"INT"
        |            },
        |            "gender":{
        |                "isMain":"N",
        |                "name":"gender",
        |                "type":"STRING"
        |            },
        |            "contry":{
        |                "isMain":"N",
        |                "name":"contry",
        |                "type":"STRING"
        |            },
        |            "province":{
        |                "isMain":"N",
        |                "name":"province",
        |                "type":"STRING"
        |            },
        |            "object_key":{
        |                "isMain":"N",
        |                "name":"object_key",
        |                "type":"STRING"
        |            }
        |        },
        |        "name":"com_new_vertex_tv_user_shanghai_1",
        |        "type":"vertex"
        |    }
        |}
        |""".stripMargin

    if (args.length > 1) {
      jsonStr = LocalFileUtils.readFile2String(args(1))
    }

    // Sink参数解析
    SinkContext.parseArgs(jsonStr)
    // 获取Sink列表
    val sinksList = SinkContext.getSinks()
    println(s"println -> Sink List[$sinksList]")
    logger.debug(s"debug -> Sink List[$sinksList]")
    logger.info(s"info -> Sink List[$sinksList]")

    var schemaStr =
      """
        |{
        |  "name": "GenericRecord",
        |  "type": "record",
        |  "namespace": "com.haizhi.volans",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "long"
        |    },
        |    {
        |      "name": "gender",
        |      "type": "string"
        |    },
        |    {
        |      "name": "country",
        |      "type": "string"
        |    },
        |    {
        |      "name": "province",
        |      "type": "string"
        |    },
        |    {
        |      "name": "object_key",
        |      "type": "string"
        |    },
        |    {
        |      "name": "_operation",
        |      "type": "string"
        |    }
        |  ]
        |}
        |""".stripMargin

    // Add Sink
    sinksList.foreach(sink => {
      if (sink.isInstanceOf[HiveSink]) {
        val hiveSink = sink.asInstanceOf[HiveSink]
        if (HiveStoreType.ORC.equals(hiveSink.getTableStoredType)) {
          val fieldSchemaList = hiveSink.getFieldSchemaList()
          stream.flatMap(_.toIterable)
            .map(new OrcConvertMapFunction(fieldSchemaList))
            .addSink(hiveSink.build(GenericFuncValue.GENERICROWDATA))
            .uid(hiveSink.uid)
        } else {
          val avroSchema = hiveSink.getAvroSchema()
          stream.flatMap(_.toIterable)
            .map(new AvroConvertMapFunction(avroSchema))
            .addSink(sink.build(GenericFuncValue.GENERICRECORD)).uid(sink.uid)
        }
      } else if (sink.isInstanceOf[FileHandleSink]) {
        stream.addSink(sink.build(GenericFuncValue.ITERABLE_STRING))
          .uid(sink.uid)
          .setParallelism(1)
      } else {
        val richSink = sink.build(GenericFuncValue.ITERABLE_STRING)
        stream.addSink(richSink).uid(sink.uid)
      }
    })

    senv.execute("Flink Hive Test")
  }

  def validateAndMerge(element: java.util.Map[String, Object]): Unit = {
    if (element.containsKey(Keys.OBJECT_KEY) && !element.containsKey(Keys.ID)) {
      element.put(Keys.ID, element.get(Keys.OBJECT_KEY))
    }
  }

}