package com.haizhi.volans.loader.scala.operator

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.parameter.Parameter
import com.haizhi.volans.loader.scala.config.streaming.StreamingConfig
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * 自定义keybeFunction核心类
 *
 * @author gl
 */
class KeyedStateFunction(streamingConfig: StreamingConfig) extends
  KeyedProcessFunction[Int, (String, Int), Iterable[String]] with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[KeyedStateFunction])

  //不同分区下的度量集合
  private val counter_consume_list: ArrayBuffer[Counter] = ArrayBuffer[Counter]()
  private val counter_dirty_list: ArrayBuffer[Counter] = ArrayBuffer[Counter]()

  // 不同key分区下的消费数量
  lazy val consumeCount: ValueState[Long] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("consumeCount", Types.of[Long])
    )

  // 不同key分区下的脏数据数量
  lazy val dirtyCount: ValueState[Long] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("dirtyCount", Types.of[Long])
    )

  // 不同key分区下的正确数据集合
  lazy val dataList: ListState[String] =
    getRuntimeContext.getListState(
      new ListStateDescriptor[String]("dataList", Types.of[String])
    )

  // 判断是否开启下一个批量处理时间计时器, 初始化默认为null
  lazy val flagTime: ValueState[Boolean] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("flagTime", Types.of[Boolean])
    )

  //定义侧输出流标签
  lazy val freezingAlarmOutput: OutputTag[String] =
    new OutputTag[String]("dirty-out")

  override def open(parameters: Configuration): Unit = {
    for (key <- 0 until streamingConfig.flinkConfig.parallelism) {
      val counter_consumeCount = getRuntimeContext
        .getMetricGroup
        .addGroup("volans")
        .counter(s"counter-consumeCount${key}")
      counter_consume_list.append(counter_consumeCount)
      val counter_dirtyCount = getRuntimeContext
        .getMetricGroup
        .addGroup("volans")
        .counter(s"counter-dirtyCount${key}")
      counter_dirty_list.append(counter_dirtyCount)
    }
  }

  override def processElement(value: (String, Int),
                              ctx: KeyedProcessFunction[Int, (String, Int), Iterable[String]]#Context,
                              out: Collector[Iterable[String]]): Unit = {
    logger.info(s"打印：key = ${value._2}, consumeCount = ${consumeCount.value()} , dirtyCount = ${dirtyCount.value()} , flagTime = ${flagTime.value()} , dataList = ${dataList.get()} ")
    if (flagTime.value() == null || !flagTime.value()) {
      logger.info(s"设置计时器，${streamingConfig.flinkConfig.batchInterval} 后执行回调函数")
      val timerTs = ctx.timerService().currentProcessingTime() + streamingConfig.flinkConfig.batchInterval
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      flagTime.update(true)
    }
    //获取脏数据度量、消费数据度量
    val dirty_counter = counter_dirty_list(value._2)
    val consume_counter = counter_consume_list(value._2)
    //当从savepoint点恢复时将状态赋予度量值
    if (dirtyCount.value() > dirty_counter.getCount)
      dirty_counter.inc(dirtyCount.value() - dirty_counter.getCount)
    if (consumeCount.value() > consume_counter.getCount)
      consume_counter.inc(consumeCount.value() - consume_counter.getCount)

    //数据校验及转换
    val tuple: (String, Boolean) = checkValue(value._1)
    //脏数据和正确数据放入不同State
    if (!tuple._2) {
      //打印脏数据
      logger.info(s"打印脏数据 key =${value._2} , dirtyData = ${tuple._1}")
      //脏数据累加
      dirtyCount.update(dirtyCount.value() + 1)
      //度量累加
      dirty_counter.inc()

      //如果脏数据设置errorMode模式>=0 且脏数据个数大于规定则程序结束
      if (streamingConfig.dirtySink.errorMode >= 0 &&
        (dirtyCount.value() * streamingConfig.flinkConfig.parallelism > streamingConfig.dirtySink.errorMode))
        throw new VolansCheckException(s"${ErrorCode.DIRTY_COUNT_ERROR}${ErrorCode.PATH_BREAK}  " +
          s"脏数据超出 errorMode [${streamingConfig.dirtySink.errorMode}] 限制")

      //开启脏数据记录 且 脏数据量小于errorStoreRowsLimit，将脏数据输出
      if (streamingConfig.dirtySink.errorStoreEnabled && (dirtyCount.value() * streamingConfig.flinkConfig.parallelism) <=
        streamingConfig.dirtySink.errorStoreRowsLimit)
        ctx.output(freezingAlarmOutput, tuple._1)

      logger.info(s"打印：key = ${value._2}, 度量脏数据总量 = ${dirty_counter.getCount}")
    } else
      dataList.add(tuple._1)

    //消费数量累加
    consumeCount.update(consumeCount.value() + 1)
    //度量累加
    consume_counter.inc()
    logger.info(s"打印：key = ${value._2}, 度量总量 = ${consume_counter.getCount}")
  }

  //触发计时器
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Int, (String, Int), Iterable[String]]#OnTimerContext,
                       out: Collector[Iterable[String]]): Unit = {
    //通过计时器将缓存数据输出
    logger.info(s" onTimer dataList = ${dataList.get()}")
    //输出正确数据
    out.collect(dataList.get().asScala)
    //清空数据并将flag改为false，重新触发计时器
    dataList.clear()
    flagTime.update(false)
  }

  //检验脏数据,false为脏数据
  def checkValue(value: String): (String, Boolean) = {
    if (value.startsWith("checkFalse")) {
      value -> false
    } else
      typeConversion(value)
  }

  //类型转换,false为转换异常
  def typeConversion(value: String): (String, Boolean) = {
    if (value.startsWith("typeFalse")) {
      value -> false
    } else
      value -> true
  }

}
