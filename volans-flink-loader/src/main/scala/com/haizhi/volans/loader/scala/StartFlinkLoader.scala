package com.haizhi.volans.loader.scala

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import config.check.{StreamingConfigHelper, StreamingExecutorHelper}
import com.haizhi.volans.loader.scala.config.streaming.{FileConfig, StreamingConfig}
import com.haizhi.volans.loader.scala.executor._
import com.haizhi.volans.loader.scala.operator.KeyedStateFunction
import com.haizhi.volans.sink.component._
import com.haizhi.volans.sink.config.constant.CoreConstants
import com.haizhi.volans.sink.func.{AvroConvertMapFunction, GenericFuncValue}
import config.schema.Keys
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{DefaultRollingPolicy, OnCheckpointRollingPolicy}
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.JavaConverters._
import scala.util.Random

object StartFlinkLoader {

  private val logger: Logger = LoggerFactory.getLogger(classOf[StartFlinkLoader])
  var logExecutor: LogExecutor = _
  var streamingConfig: StreamingConfig = _
  var streamingExecutor: StreamingExecutor = _

  def main(args: Array[String]): Unit = {
    try {
      /*System.setProperty("HADOOP_USER_NAME", "work")
      System.setProperty("user.name", "work")*/
      // 加载参数
      initArgsExecutor(args)
      //sinks参数解析
      SinkContext.parseArgs(streamingConfig.sinks)
      // Flink上下文
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      // 配置checkPoint
      setCheckPoint(env)
      // 配置重启策略
      setRestart(env)
      val parallelism = streamingConfig.flinkConfig.parallelism
      val stream: DataStream[Iterable[String]] = env
        .addSource(getKafkaSource)
        .uid("source-id")
        .map(_ -> Random.nextInt(parallelism).toString)
        .uid("map-id")
        .keyBy(_._2)
        .process(new KeyedStateFunction(streamingConfig))
        .uid("process-id")

      // 通过侧输出流获取脏数据并输出
      if (streamingConfig.errorInfo.dirtyData.storeEnabled) {
        logger.info(" open errorStoreEnabled is true ")
        stream
          .getSideOutput(new OutputTag[String]("dirty-out"))
          .addSink(getDirtySink).setParallelism(1)
          .uid("dirtySink-id")
      }

      // 获取Sink列表, 循环增加sink
      val sinksList: List[Sink] = SinkContext.getSinks()
      sinksList.foreach {
        case sink@(_: HiveSink) =>
          val avroSchema = sink.config.getProperty(CoreConstants.AVRO_SCHEMA)
          stream.flatMap(_.toIterable)
            .map(new AvroConvertMapFunction(avroSchema))
            .addSink(sink.build(GenericFuncValue.GENERICRECORD)).uid(sink.uid)
        case sink@(_: FileHandleSink) =>
          stream.addSink(sink.build(GenericFuncValue.ITERABLE_STRING))
            .uid(sink.uid)
            .setParallelism(1)
        case sink =>
          val richSink = sink.build(GenericFuncValue.ITERABLE_STRING)
          stream.addSink(richSink).uid(sink.uid)
      }
      //执行程序
      env.execute()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        doLogHappenError(e)
        throw e
    }
  }

  /**
   * 设置脏数据输出FileSink
   * 由于生产环境大部分hadoop版本 < 2.7 所以使用 OnCheckpointRollingPolicy 滚动策略
   * 默认滚动和checkpoint时间保持一致
   *
   * 支持路径：hdfs://localhost:9000 和 本地路径: file:///User/hzxt/log
   *
   * 注意：当侧输出流中没有数据时不会生成新桶，亦不会生成新文件
   * 同一目录下一个文件自增ID只会生成一个成功的文件，例如:dirty-2-0, dirty-2-1, dirty-2-2, dirty-2-3, dirty-2-4, dirty-2-5
   */
  def getDirtySink: StreamingFileSink[String] = {
    val config = OutputFileConfig
      .builder()
      .withPartPrefix("dirty")
      .build()

    val fileConfig: FileConfig = streamingConfig.errorInfo.dirtyData.dirtyConfig.asInstanceOf[FileConfig]
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(fileConfig.path), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd")) //配置桶生成规则, 按天生成
      .withRollingPolicy(
        OnCheckpointRollingPolicy.build()
      )
      .withOutputFileConfig(config)
      .build()
    sink
  }

  /**
   * checkPoint 支持路径：hdfs://localhost:9000 或本地路径: file:///User/hzxt/log
   */
  def setCheckPoint(env: StreamExecutionEnvironment): Unit = {
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //设置全局并行度
    env.setParallelism(streamingConfig.flinkConfig.parallelism)
    //设置检查点，默认异步提交，使用FS作为状态后端
    val stateBackend: StateBackend = new FsStateBackend(streamingConfig.flinkConfig.checkPoint)
    env.setStateBackend(stateBackend)
    // 每 1000ms * 60 开始一次 checkpoint
    env.enableCheckpointing(streamingConfig.flinkConfig.checkpointInterval)
    // 设置模式为精确一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确认 checkpoints 之间的最短时间间隔会进行 5000 ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许一个 checkpoint 进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 允许在有更近 savepoint 时回退到 checkpoint
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    // 可容忍检查点失败个数：3
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    // 开启在 job 中止后仍然保留的 externalized checkpoints
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  }

  def setRestart(env: StreamExecutionEnvironment): Unit = {
    // 设置重启策略为固定重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      streamingConfig.flinkConfig.restart, // 重启尝试次数，每两次连续的重启尝试之间等待 10 秒钟。
      org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
    ))
  }

  /**
   * 创建kafkaConsumer
   *
   * @return
   */
  def getKafkaSource: FlinkKafkaConsumer[String] = {
    val properties: Properties = new Properties()
    val kafkaSourceConfig = streamingConfig.source.kafkaSourceConfig
    properties.setProperty("bootstrap.servers", kafkaSourceConfig.servers)
    properties.setProperty("group.id", kafkaSourceConfig.groupId)
    properties.setProperty("session.timeout.ms", "60000")
    kafkaSourceConfig.config.asScala.foreach {
      case (key, value) =>
        properties.setProperty(key, String.valueOf(value))
    }

    //connect Kafka, 可支持kafka >= 1.0.0
    val myConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(
      kafkaSourceConfig.topic
      , new SimpleStringSchema()
      , properties
    )
    myConsumer
  }

  /**
   * 加载全局参数
   */
  def initArgsExecutor(args: Array[String]): Unit = {
    logger.info("=========================================================================")
    logger.info("===================Flink Streaming Application Start=====================")
    logger.info("=========================================================================")
    val parameter = ParameterTool.fromArgs(args)
    val path = parameter.get("input", null)
    //加载全局参数
    streamingConfig = StreamingConfigHelper.parse(path)
    //构建streamingExecutor
    streamingExecutor = StreamingExecutorHelper.buildExecutors(streamingConfig)
    logExecutor = streamingExecutor.logExecutor
    //循环初始化
    streamingExecutor.forInit()
  }

  /**
   * 将异常信息写入errorSink
   */
  def doLogHappenError(e: Exception): Unit = {
    val errorLog: String = ErrorCode.toJSON(task_instance_id = Keys.taskInstanceId, code = ErrorCode.STREAMING_ERROR, message = e.getMessage)
    logger.error(errorLog)
    if (logExecutor != null) //需要用errorExceutor的errorWriter写出异常数据
      logExecutor.LogWriter(errorLog)
    else if (Keys.logInfo != null) //如果errorSink存在，代表errorSink配置后面出现了异常，可以先生成errorExcecutor
      StreamingExecutorHelper.getLogExecutor(Keys.logInfo).LogWriter(errorLog)
  }

  case class StartFlinkLoader()

}
