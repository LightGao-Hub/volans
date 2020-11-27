package com.haizhi.volans.loader.scala.executor

import com.haizhi.volans.common.flink.base.java.util.FileUtil
import com.haizhi.volans.common.flink.base.scala.exception.ErrorCode
import com.haizhi.volans.loader.scala.config.exception.VolansPathException
import com.haizhi.volans.loader.scala.config.schema.Keys
import com.haizhi.volans.loader.scala.config.streaming.FileConfig
import com.haizhi.volans.loader.scala.util.HDFSUtils
import org.slf4j.{Logger, LoggerFactory}

case class FileExecutor(fileConfig: FileConfig) extends LogExecutor with DirtyExecutor {

  private val LOG: Logger =  LoggerFactory.getLogger(classOf[FileExecutor])

  override def init(): Unit = {
    LOG.info(s" FileExecutor init success path = ${fileConfig.path} ")
  }

  override def close(): Unit = {
    LOG.info(s" FileExecutor close success ")
  }

  override def LogWriter(value: String): Unit = {
    LOG.info(s" errorWriter info path = ${fileConfig.path}; value = $value")
    writeFileOrHdfs(value, fileConfig.path.trim, s"${Keys.ERROR_LOG}")
    LOG.info(" errorWriter success ")
  }

  /**
   * 读取path文件下的数据
   *
   * @param path
   * @return 当目录不存在时，返回值为null
   */
  def readFileOrHdfs(path: String): String = {
    checkPath(path)
    val newPath = pathHelper(path, s"${Keys.COUNT_LOG}")
    LOG.info(s" newPath = $newPath")
    var value: String = null
    if (newPath.startsWith(Keys.FILE_PATH)) {
      value = FileUtil.readFileToString(newPath, s"${Keys.ENCODING}")
    } else if (newPath.startsWith(Keys.HDFS_PATH)) {
      value = HDFSUtils.readFileContent(newPath.replace(Keys.HDFS_PATH, ""))
    } else if (newPath.startsWith(Keys.PATH_FLAG)) {
      value = HDFSUtils.readFileContent(newPath)
    }
    value
  }

  /**
   * 输出到本地file或hdfs，此函数特点是无法append追加，每次文件只会覆盖数据，支持自动创建文件和目录
   *
   * @param content 内容
   * @param path    路径
   */
  def writeFileOrHdfs(content: String, path: String, model: String): Unit = {
    checkPath(path)
    val newPath = pathHelper(path, s"$model")
    LOG.info(s" newPath = $newPath")
    if (newPath.startsWith(s"${Keys.FILE_PATH}")) {
      FileUtil.writeStringToFile(newPath, content, s"${Keys.ENCODING}", false)
    } else if (newPath.startsWith(s"${Keys.HDFS_PATH}")) {
      HDFSUtils.upload(content, newPath)
    } else if (newPath.startsWith(s"${Keys.PATH_FLAG}")) {
      HDFSUtils.upload(content, newPath)
    }
  }

  /**
   * 将脏数据追加到文件中, 文件或目录不存在会自动创建
   *
   * @param content
   * @param path
   * @param model
   */
  def appendFileOrHdfs(content: String, path: String, model: String): Unit = {
    checkPath(path)
    val newPath = pathHelper(path, s"$model")
    LOG.info(s" newPath = $newPath")
    if (newPath.startsWith(s"${Keys.FILE_PATH}")) {
      FileUtil.writeStringToFile(newPath, content, s"${Keys.ENCODING}", true)
    } else if (newPath.startsWith(s"${Keys.HDFS_PATH}")) {
      HDFSUtils.writeAppendFileAsTxt(content, newPath)
    } else if (newPath.startsWith(s"${Keys.PATH_FLAG}")) {
      HDFSUtils.writeAppendFileAsTxt(content, newPath)
    }
  }

  /**
   * 兼容路径是否以error.log结尾，例如：
   *        1. xxx/path/
   *        2. xxx/path
   *        3. xxx/path/error.log
   *
   * @param path
   * @return
   */
  def pathHelper(path: String, model: String): String = {
    if (path.endsWith(s"$model")) {
      path
    } else {
      if (path.endsWith("/"))
        path + s"$model"
      else
        path + s"${Keys.PATH_FLAG}$model"
    }
  }

  def checkPath(path: String): Unit = {
    if (!path.startsWith(s"${Keys.FILE_PATH}") && !path.startsWith(s"${Keys.HDFS_PATH}") && !path.startsWith(s"${Keys.PATH_FLAG}")) {
      throw new VolansPathException(s"${ErrorCode.PARAMETER_CHECK_ERROR}${ErrorCode.PATH_BREAK} the path  [$path] is error ")
    }
  }

  override def dirtyWriter(value: String): Unit = {

  }
}
