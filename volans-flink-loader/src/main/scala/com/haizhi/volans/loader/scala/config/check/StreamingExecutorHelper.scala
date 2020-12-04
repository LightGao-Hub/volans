package com.haizhi.volans.loader.scala.config.check

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.streaming.{FileConfig, StreamingConfig}
import com.haizhi.volans.loader.scala.config.streaming.error._
import com.haizhi.volans.loader.scala.executor._
import com.hzxt.volans.loader.java.StoreType
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author gl
 * @create 2020-11-04 13:52
 */
object StreamingExecutorHelper {

  private val LOG: Logger =  LoggerFactory.getLogger(classOf[StreamingExecutorHelper])

  /**
   * 构建Executor
   * @return
   */
  def buildExecutors(streamingConfig: StreamingConfig): StreamingExecutor = {
    try {
      val logExecutor = getLogExecutor(streamingConfig.errorInfo.logInfo)
      LOG.info(s" errorExecutor = $logExecutor")
      val dirtyExecutor = getDirtyExecutor(streamingConfig.errorInfo.dirtyData)
      LOG.info(s" dirtyExecutor = $dirtyExecutor")
      StreamingExecutor(logExecutor, dirtyExecutor)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK}  ${ErrorCode.getJSON(e.getMessage)} <-  buildExecutors 函数构建StreamingExecutor异常")
    }
  }

  def getLogExecutor(logInfo: LogInfo): LogExecutor = {
    logInfo.storeType match {
      case StoreType.FILE => FileExecutor(logInfo.logInfoConfig.asInstanceOf[FileConfig])
      case _ => throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} getErrorExecutor [${logInfo.storeType}] 类型不存在 ")
    }
  }

  def getDirtyExecutor(dirtyData: DirtyData): DirtyExecutor = {
    dirtyData.storeType match {
      case StoreType.FILE => FileExecutor(dirtyData.dirtyConfig.asInstanceOf[FileConfig])
      case _ => throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} getDirtyExecutor [${dirtyData.storeType}] 类型不存在 ")
    }
  }

  case class StreamingExecutorHelper()

}
