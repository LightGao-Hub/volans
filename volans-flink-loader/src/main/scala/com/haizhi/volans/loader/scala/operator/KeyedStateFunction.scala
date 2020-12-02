package com.haizhi.volans.loader.scala.operator

import java.util

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.check.CheckValueConversion
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
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

/**
 * 自定义keybeFunction核心类
 * 注意：度量指标不会通过检查点恢复，此外ontimer期间如果中断下次重启会先执行open 后执行ontimer，open优先级高于重启后的ontimer
 * @author gl
 */
class KeyedStateFunction(streamingConfig: StreamingConfig) extends
  KeyedProcessFunction[String, (String, String), Iterable[String]] with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[KeyedStateFunction])

  //不同分区下的度量集合
  private var consume_counter: Counter = _
  private var dirty_counter: Counter = _
  //数据检查和转换类
  private val check = CheckValueConversion(streamingConfig)

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
    logger.info(" open info ")
    //获取脏数据度量、消费数据度量
    if(consume_counter == null) {
      consume_counter = getRuntimeContext
        .getMetricGroup
        .addGroup("volans")
        .counter(s"consumeCount${getRuntimeContext.getIndexOfThisSubtask}")
    }
    if(dirty_counter == null) {
      dirty_counter = getRuntimeContext
        .getMetricGroup
        .addGroup("volans")
        .counter(s"dirtyCount${getRuntimeContext.getIndexOfThisSubtask}")
    }
  }

  override def processElement(value: (String, String),
                              ctx: KeyedProcessFunction[String, (String, String), Iterable[String]]#Context,
                              out: Collector[Iterable[String]]): Unit = {
    logger.info(s"打印：key = ${value._2}, 线程 = ${Thread.currentThread().getName} ,consumeCount = ${consumeCount.value()} , dirtyCount = ${dirtyCount.value()} , flagTime = ${flagTime.value()} , dataList = ${dataList.get()} ")
    if (flagTime.value() == null || !flagTime.value()) {
      logger.info(s"设置计时器，${streamingConfig.flinkConfig.batchInterval} 后执行回调函数")
      val timerTs = ctx.timerService().currentProcessingTime() + streamingConfig.flinkConfig.batchInterval
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      flagTime.update(true)
    }
    //当从savepoint点恢复时将状态赋予度量值
    if (dirtyCount.value() > dirty_counter.getCount)
      dirty_counter.inc(dirtyCount.value() - dirty_counter.getCount)
    if (consumeCount.value() > consume_counter.getCount)
      consume_counter.inc(consumeCount.value() - consume_counter.getCount)

    dataList.add(value._1)
    //消费数量累加
    consumeCount.update(consumeCount.value() + 1)
    //度量累加
    consume_counter.inc()
    logger.info(s"info：key = ${value._2}, 度量总量 = ${consume_counter.getCount}, 脏数据总量 = ${dirty_counter.getCount}")
  }

  //触发计时器
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, (String, String), Iterable[String]]#OnTimerContext,
                       out: Collector[Iterable[String]]): Unit = {
    //通过计时器将缓存数据输出
    logger.info(s" onTimer check start dataList = ${dataList.get()}")
    val iter: util.Iterator[String] = dataList.get().iterator()
    val listIterable = new util.ArrayList[String]
    while (iter.hasNext) {
      val value: String = iter.next()
      //数据校验及转换
      val tuple: (String, Boolean) = check.checkValue(value)
      //脏数据和正确数据放入不同State
      if (!tuple._2) {
        //打印脏数据
        logger.info(s"打印脏数据 dirtyData = ${tuple._1}")
        //脏数据累加
        dirtyCount.update(dirtyCount.value() + 1)
        //度量增加
        dirty_counter.inc()
        //开启脏数据记录 且 脏数据量小于errorStoreRowsLimit，将脏数据输出
        if (streamingConfig.errorInfo.dirtyData.storeEnabled && (dirtyCount.value() * streamingConfig.flinkConfig.parallelism) <=
          streamingConfig.errorInfo.dirtyData.storeRowsLimit)
          ctx.output(freezingAlarmOutput, tuple._1)
        //然后将脏数据删除
        iter.remove()
      } else
        listIterable.add(tuple._1)
    }
    logger.info(s" onTimer check end dataList size = ${listIterable.size()}")
    //输出正确数据
    out.collect(listIterable.asScala)
    //清空数据并将flag改为false，重新触发计时器
    dataList.clear()
    flagTime.update(false)
    //如果脏数据设置errorMode模式>=0 且脏数据个数大于规定则程序结束
    if (streamingConfig.errorInfo.dirtyData.handleMode >= 0 &&
      (dirtyCount.value() * streamingConfig.flinkConfig.parallelism > streamingConfig.errorInfo.dirtyData.handleMode))
      throw new VolansCheckException(s"${ErrorCode.DIRTY_COUNT_ERROR}${ErrorCode.PATH_BREAK}  " +
        s"脏数据超出 errorMode [${streamingConfig.errorInfo.dirtyData.handleMode}] 限制")
  }

}
