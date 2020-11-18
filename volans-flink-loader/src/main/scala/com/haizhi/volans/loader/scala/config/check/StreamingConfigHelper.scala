package com.haizhi.volans.loader.scala.config.check

import java.lang.reflect.Type
import java.util

import com.google.gson.reflect.TypeToken
import com.haizhi.volans.common.flink.base.java.util.FileUtil
import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.common.flink.base.scala.util.JSONUtils
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.parameter.Parameter
import com.haizhi.volans.loader.scala.config.schema.{Keys, SchemaVo}
import com.haizhi.volans.loader.scala.config.streaming.{FileConfig, StoreType, StreamingConfig}
import com.haizhi.volans.loader.scala.config.streaming.dirty.DirtySink
import com.haizhi.volans.loader.scala.config.streaming.error.ErrorSink
import com.haizhi.volans.loader.scala.config.streaming.flink.FlinkConfig
import com.haizhi.volans.loader.scala.config.streaming.sink.Sinks
import com.haizhi.volans.loader.scala.config.streaming.source.KafkaSourceConfig
import com.haizhi.volans.loader.scala.util.HDFSUtils
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author gl 
 * @create 2020-11-02 13:52 
 */
object StreamingConfigHelper {
  private val LOG: Logger =  LoggerFactory.getLogger(classOf[StreamingConfigHelper])
  /**
   * 根据main函数args传参，创建全局streamingConfig
   *
   * @param inputParam args[]
   */
  def parse(inputParam: Array[String]): StreamingConfig = {
    if (null == inputParam || inputParam.length == 0) {
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} inputParam args[] 参数有误")
    }
    val content: String = downloadParamIfNecessary(inputParam(0))
    if (StringUtils.isBlank(content))
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} inputParam args[] 参数类型有误， 无法获取全局参数")
    LOG.info("SparkArgs：" + content)
    doParse(content)
  }

  /**
   * 根据启动参数获取不同位置的全局配置
   *
   * @param inputParamPath args[0]
   * @return 返回json字符串
   */
  private def downloadParamIfNecessary(inputParamPath: String): String = {
    var content: String = null
    LOG.info(s"flink driver args: ${inputParamPath}")
    if (inputParamPath.startsWith("/") || inputParamPath.startsWith("file:///") || inputParamPath.startsWith("file:/")) {
      content = FileUtil.readFileToString(inputParamPath, "utf-8")
    } else if (inputParamPath.startsWith("{")) {
      content = inputParamPath
    } else if (inputParamPath.startsWith("hdfs://")) {
      content = HDFSUtils.readFileContent(inputParamPath)
    } else {
      content = FileUtil.readThisPath(inputParamPath)
    }
    content
  }


  def getSchema(map: util.Map[String, AnyRef]): SchemaVo = {
    val schemaVo: SchemaVo = JSONUtils.fromJson(JSONUtils.toJson(map.get(Parameter.SCHEMA)), new TypeToken[SchemaVo]() {}.getType)
    schemaVo
  }

  /**
   * 执行函数，通过json参数创建全局配置类：streamingConfig
   *
   * @param param json参数
   */
  def doParse(param: String): StreamingConfig = {
    val map: util.Map[String, AnyRef] = JSONUtils.jsonToMap(param)
    //检查关键参数
    CheckHelper.checkMap(map)
    //获取error
    val errorSink: ErrorSink = getErrorSink(map)
    Keys.errorSink = errorSink
    LOG.info(s" info ${errorSink.storeType} error : $errorSink")
    //先获取dirty，因为后面任何异常都需要taskInstanceId参数才可展示异常
    val dirtySink: DirtySink = getDirtySink(map)
    Keys.taskInstanceId = dirtySink.taskInstanceId
    LOG.info(s" info ${dirtySink.storeType} dirty : $dirtySink")
    //获取source
    val kafkaSource: KafkaSourceConfig = getSource(map)
    LOG.info(s" info ${kafkaSource.storeType} source : $kafkaSource")
    //获取sinksJson
    val sinksJson: String = getSinks(map)
    LOG.info(s" info sinks Json : $sinksJson")
    //获取schema
    val schemaVo: SchemaVo = getSchema(map)
    LOG.info(s" info schemaVo : $schemaVo")
    //获取checkpoint
    CheckHelper.checkNotNull(MapUtils.getString(map, Parameter.CHECKPOINT), Parameter.CHECKPOINT, taskId = Keys.taskInstanceId)
    val checkPoint: String = map.get(Parameter.CHECKPOINT).toString
    LOG.info(s" info checkpoint : $checkPoint")
    //获取flinkConfig
    val flinkConfig: FlinkConfig = getFlinkConfig(map)
    LOG.info(s" info flinkConfig : $flinkConfig")

    StreamingConfig(kafkaSource, sinksJson, schemaVo, errorSink, dirtySink, checkPoint, flinkConfig)
  }

  /**
   * 生成对应的kafkaSource
   *
   * @param map 全局参数map
   * @return source类
   */
  def getSource(map: util.Map[String, AnyRef]): KafkaSourceConfig = {
    val sourceMap: util.Map[String, AnyRef] = JSONUtils.jsonToMap(JSONUtils.toJson(map.get(Parameter.SOURCE)))
    CheckHelper.checkNotNull(MapUtils.getString(sourceMap, Parameter.STORE_TYPE), Parameter.STORE_TYPE, taskId = Keys.taskInstanceId)
    val storeType: StoreType = StoreType.findStoreType(MapUtils.getString(sourceMap, Parameter.STORE_TYPE))
    if (storeType != StoreType.KAFKA)
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} source [$storeType] 类型不存在 ")

    val kafka_server = sourceMap.get(Parameter.KAFKA_SERVER).asInstanceOf[String]
    var kafka_groupid = sourceMap.get(Parameter.KAFKA_GROUPID).asInstanceOf[String]
    if(StringUtils.isBlank(kafka_groupid))
      kafka_groupid = "flink-loader"
    val topic = sourceMap.get(Parameter.TOPIC).asInstanceOf[String]
    KafkaSourceConfig(storeType, kafka_server, kafka_groupid, topic)
  }

  /**
   * 根据storeType不同生成对应的dirtySink
   *
   * @param map 全局参数map
   * @return dirtySink
   */
  def getDirtySink(map: util.Map[String, AnyRef]): DirtySink = {
    val dirtyMap: util.Map[String, AnyRef] = JSONUtils.jsonToMap(JSONUtils.toJson(map.get(Parameter.DIRTY_SINK)))
    CheckHelper.checkNotNull(MapUtils.getString(dirtyMap, Parameter.STORE_TYPE), Parameter.STORE_TYPE)
    val storeType: StoreType = StoreType.findStoreType(MapUtils.getString(dirtyMap, Parameter.STORE_TYPE))
    var typeOfT: Type = null
    if (storeType == StoreType.FILE)
      typeOfT = new TypeToken[FileConfig]() {}.getType
    else
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} dirtySink [$storeType] 类型不存在 ")

    CheckHelper.checkNotNull(MapUtils.getString(dirtyMap, Parameter.DIRTY_CONFIG), Parameter.DIRTY_CONFIG)
    val errorMode = dirtyMap.getOrDefault(Parameter.ERROR_MODE, Long.box(-1L)).asInstanceOf[Long]
    val errorStoreEnabled = dirtyMap.getOrDefault(Parameter.ERROR_STORE_ENABLED, Boolean.box(false)).asInstanceOf[Boolean]
    val errorStoreRowsLimit = dirtyMap.getOrDefault(Parameter.ERROR_STORE_FOWSLIMIT, Long.box(30000)).asInstanceOf[Long]
    val inboundTaskId = dirtyMap.get(Parameter.INBOUND_TASKID).asInstanceOf[String]
    val taskInstanceId = dirtyMap.get(Parameter.TASK_INSTANCEID).asInstanceOf[String]

    DirtySink(storeType, errorMode, errorStoreEnabled, errorStoreRowsLimit, inboundTaskId, taskInstanceId, JSONUtils.fromJson(JSONUtils.toJson(dirtyMap.get(Parameter.DIRTY_CONFIG)), typeOfT))
  }

  /**
   * 根据storeType不同生成对应的errorSink
   *
   * @param map 全局参数map
   * @return errorSink
   */
  def getErrorSink(map: util.Map[String, AnyRef]): ErrorSink = {
    val errorMap: util.Map[String, AnyRef] = JSONUtils.jsonToMap(JSONUtils.toJson(map.get(Parameter.ERROR_SINK)))
    CheckHelper.checkNotNull(MapUtils.getString(errorMap, Parameter.STORE_TYPE), Parameter.STORE_TYPE, taskId = Keys.taskInstanceId)
    val storeType: StoreType = StoreType.findStoreType(MapUtils.getString(errorMap, Parameter.STORE_TYPE))
    var typeOfT: Type = null
    if (storeType == StoreType.FILE)
      typeOfT = new TypeToken[FileConfig]() {}.getType
    else
      throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} errorSink [$storeType] 类型不存在 ")
    CheckHelper.checkNotNull(MapUtils.getString(errorMap, Parameter.ERROR_CONFIG), Parameter.ERROR_CONFIG, taskId = Keys.taskInstanceId)
    ErrorSink(storeType, JSONUtils.fromJson(JSONUtils.toJson(errorMap.get(Parameter.ERROR_CONFIG)), typeOfT))
  }

  /**
   * 获取flinkConfig
   *
   * @param map 全局参数map
   * @return flinkConfig
   */
  def getFlinkConfig(map: util.Map[String, AnyRef]): FlinkConfig = {
    val flinkConfing: FlinkConfig = JSONUtils.fromJson(JSONUtils.toJson(map.get(Parameter.FLINK_CONFIG)), new TypeToken[FlinkConfig]() {}.getType)
    flinkConfing.check
    flinkConfing
  }


  /**
   * 获取sinks schema 的json字符串
   *
   * @param map 全局参数map
   * @return sparkConfig
   */
  def getSinks(map: util.Map[String, AnyRef]): String = {
    CheckHelper.checkSinks(map)
    CheckHelper.checkSchema(map)
    val sinks: Sinks = Sinks(map.get(Parameter.SINKS), map.get(Parameter.SCHEMA))
    JSONUtils.toJson(sinks)
  }


  case class StreamingConfigHelper()

}
