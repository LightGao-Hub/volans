package com.haizh.volans.sink

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

/**
 * Author pengxb
 * Date 2020/11/11
 */
object StreamToHive {

  def main(args: Array[String]): Unit = {

    /**
     * 1.建Hive表bigdata_test.users，语句如下：
     * CREATE EXTERNAL TABLE IF NOT EXISTS bigdata_test.users(
     * name string,
     * age int,
     * gender string
     * )
     * PARTITIONED BY(country string,province string)
     * ROW FORMAT DELIMITED
     * FIELDS TERMINATED BY ','
     * STORED AS TEXTFILE
     * LOCATION 'hdfs://nameservice1/tmp/work/bigdata/flink/users';
     */

//    System.setProperty("HADOOP_USER_NAME", "work")

    /**
     * 2.构建StreamEnv,设置CheckPoint
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    setCheckPointConfig(env, "hdfs://nameservice1/tmp/work/bigdata/flink/checkpoint_users")
    setCheckPointConfig(env, "file:///Users/haizhi/workspace/train/flink/checkpoint_users")

    /**
     * 3.接入Socket Source
     *
     * 数据样例:
     * 张三,30,male,China,Beijing
     * 李四,22,male,China,Beijing
     * 王五,25,male,China,Guangzhou
     * 赵六,40,male,China,Shanghai
     * 李丽,18,famale,China,Shenzhen
     * Tom,21,male,America,Newyork
     * Jerry,27,male,France,Paris
     *
     */
    val socketDS = env.socketTextStream("localhost", 9999)

    val ds = socketDS
      .map(_->1)
      .keyBy(_._2)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
      .process(new MyProcessWindowFunction)


    /**
     * 4.使用StreamingFileSink，根据数据中的分区字段将其写入不同分区目录，并根据指定滚动策略合并小文件
     */
//    val outputPath1 = "hdfs://nameservice1/tmp/work/bigdata/flink/users"
    val outputPath1 = "file:///Users/haizhi/workspace/train/flink/output1"
    val streamSink = StreamingFileSink
      .forRowFormat(new Path(outputPath1), new SimpleStringEncoder[Iterable[String]]("UTF-8"))
      .withBucketAssigner(new BasePathBucketAssigner[Iterable[String]]{
        override def getBucketId(element: Iterable[String], context: BucketAssigner.Context): String = {
          val tokens = element.head.split(",")
          new StringBuilder()
            .append("country=")
            .append(tokens(tokens.length-2))
            .append(java.io.File.separator)
            .append("province=")
            .append(tokens(tokens.length-1)).toString()
        }
      })
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.SECONDS.toMillis(2))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
          .withMaxPartSize(1024)
          .build())

//      .build()
//    ds.addSink(streamSink)

    /**
     * 数据写入Parquet文件，策略为OnCheckpointRollingPolicy，每次checkpoint时滚动文件
     */
/*    val streamSink:StreamingFileSink[User] = StreamingFileSink
      .forBulkFormat(new Path(outputPath1),ParquetAvroWriters.forReflectRecord(classOf[User]))
      .withBucketAssigner(new BucketAssigner[User,String]{
        override def getBucketId(element: User, context: BucketAssigner.Context): String = {
          new StringBuilder()
            .append("country=")
            .append(element.country)
            .append(Path.SEPARATOR)
            .append("province=")
            .append(element.province)
            .toString()
        }
        override def getSerializer: SimpleVersionedSerializer[String] = {
          SimpleVersionedStringSerializer.INSTANCE
        }
      })
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()

    socketDS
      .map(line => {
        val tokens = line.split(",")
        User(tokens(0), tokens(1).toInt, tokens(2), tokens(3), tokens(4))
      })
      .addSink(streamSink)*/

//    socketDS.addSink(streamSink)

    /**
     * 使用BucketingSink方式，Flink 1.9后的版本已废弃
     */
    /*    val outputPath2 = "file:///Users/haizhi/workspace/train/flink/output2"
        val bucketingSink = new BucketingSink[String](outputPath2)
          .setBucketer(new CustomBasePathBucketer)
          .setBatchRolloverInterval(30 * 1000)
          .setBatchSize(1024)
        socketDS.addSink(bucketingSink)*/

    /**
     * 5.启动线程定时刷新Hive分区
     */
/*    new Thread(
      new HiveTableRefreshJob(
        "jdbc:hive2://cmb01:10000/bigdata_test",
        "",
        "",
        "bigdata_test.users", 60 * 1000
      )
    ).start()*/

    /**
     * 6.启动Flink任务
     */
    env.execute("StreamToHive")
  }

  /**
   * 配置checkpoint
   *
   * @param env
   * @param checkPointPath
   */
  def setCheckPointConfig(env: StreamExecutionEnvironment, checkPointPath: String): Unit = {
    //设置检查点，默认异步提交，使用FS作为状态后端
    //    val stateBackend: StateBackend = new FsStateBackend(checkPointPath)
    //    env.setStateBackend(stateBackend)
    // 每 1000ms * 30 开始一次 checkpoint
    env.enableCheckpointing(1000 * 30)
    // 设置模式为精确一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(20 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  }

}

case class User(name: String, age: Int, gender: String, country: String, province: String)
