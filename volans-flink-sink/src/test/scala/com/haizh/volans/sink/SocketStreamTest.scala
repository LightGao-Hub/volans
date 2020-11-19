package com.haizh.volans.sink

import com.flink.sink.ArangoDBSink
import com.haizhi.volans.sink.sinks.{HiveSink, SinkContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

/**
 * Author pengxb
 * Date 2020/11/17
 */
object SocketStreamTest {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    senv.enableCheckpointing(10000)

    /**
     * 样例数据：
     * {"name":"王五","age":22,"gender":"male","country":"China","province":"Guangzhou","object_key":"3","_operation":"INSERT"}
     * {"name":"王五","age":22,"gender":"male","country":"China","province":"Guangzhou","object_key":"3","_operation":"DELETE"}
     */
    val input: DataStream[Iterable[String]] = senv
      .socketTextStream("localhost", 9999)
      .map(_ -> 1)
      .keyBy(_._2)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .process(new MyProcessWindowFunction)

    val jsonStr =
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
        |        },
        |        {
        |            "storeType":"GDB",
        |            "storeConfig":{
        |                "database":"Graph_CDH570",
        |                "collection":"arangoDBTest",
        |                "collectionType":"vertex",
        |                "maxConnections":5,
        |                "numberOfShards":9,
        |                "password":"haizhi",
        |                "replicationFactor":1,
        |                "url":"192.168.1.37:8529",
        |                "user":"haizhi"
        |            }
        |        },
        |         {
        |            "storeType":"ES",
        |            "storeConfig":{
        |                "index":"flink_test.person",
        |                "type":"person",
        |                "url":"192.168.1.208:9200",
        |                "mapping":{
        |                    "dynamic_date_formats":[
        |                        "yyyy-MM-dd HH:mm:ss",
        |                        "yyyy-MM-dd"
        |                    ],
        |                    "dynamic_templates":[
        |                        {
        |                            "strings":{
        |                                "mapping":{
        |                                    "analyzer":"ik",
        |                                    "type":"text",
        |                                    "field s":{
        |                                        "keyword":{
        |                                            "normalizer":"my_normalizer",
        |                                            "type":"keyword"
        |                                        }
        |                                    }
        |                                },
        |                                "match_mapping_type":"string"
        |                            }
        |                        }
        |                    ],
        |                    "_all":{
        |                        "enabled":false
        |                    },
        |                    "date_detection":true
        |                },
        |                "setting":{
        |                    "analysis":{
        |                        "normalizer":{
        |                            "my_normalizer":{
        |                                "filter":[
        |                                    "lowercase",
        |                                    "asciifolding"
        |                                ],
        |                                "char_filter":[
        |
        |                                ],
        |                                "type":"custom"
        |                            }
        |                        },
        |                        "analyzer":{
        |                            "ik":{
        |                                "type":"custom",
        |                                "tokenizer":"ik_max_word"
        |                            }
        |                        }
        |                    },
        |                    "index.number_of_replicas":1,
        |                    "index.number_of_shards":5
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

    // Sink参数解析
    SinkContext.parseArgs(jsonStr)
    // 获取Sink列表
    val sinksList = SinkContext.getSinks()
    // Add Sink
    sinksList.foreach(sink => {
      if(sink.isInstanceOf[HiveSink]){
      input.flatMap(_.toIterable).addSink(sink.build(""))
      }else{
        val richSink = sink.build(Iterable(""))
        input.addSink(richSink)
      }
    })

    senv.execute("Flink Hbase Test")
  }
}
