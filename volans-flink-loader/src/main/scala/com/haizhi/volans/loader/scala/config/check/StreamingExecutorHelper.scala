package com.haizhi.volans.loader.scala.config.check

import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.exception.VolansCheckException
import com.haizhi.volans.loader.scala.config.streaming.dirty.DirtySink
import com.haizhi.volans.loader.scala.config.streaming.{FileConfig, StreamingConfig}
import com.haizhi.volans.loader.scala.config.streaming.error.ErrorSink
import com.haizhi.volans.loader.scala.executor.{DirtyExecutor, ErrorExecutor, FileExecutor, StreamingExecutor}
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
    val errorExecutor = getErrorExecutor(streamingConfig.errorSink)
    LOG.info(s" errorExecutor = $errorExecutor")
    val dirtyExecutor = getDirtyExecutor(streamingConfig.dirtySink)
    LOG.info(s" dirtyExecutor = $dirtyExecutor")
    StreamingExecutor(errorExecutor, dirtyExecutor)
  }

  def getErrorExecutor(errorSink: ErrorSink): ErrorExecutor = {
    errorSink.storeType match {
      case StoreType.FILE => FileExecutor(errorSink.errorConfig.asInstanceOf[FileConfig])
      case _ => throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} getErrorExecutor [${errorSink.storeType}] 类型不存在 ")
    }
  }

  def getDirtyExecutor(dirtySink: DirtySink): DirtyExecutor = {
    dirtySink.storeType match {
      case StoreType.FILE => FileExecutor(dirtySink.dirtyConfig.asInstanceOf[FileConfig])
      case _ => throw new VolansCheckException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} getDirtyExecutor [${dirtySink.storeType}] 类型不存在 ")
    }
  }

  case class StreamingExecutorHelper()

}
