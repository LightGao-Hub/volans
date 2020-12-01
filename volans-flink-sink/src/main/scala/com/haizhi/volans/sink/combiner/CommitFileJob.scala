package com.haizhi.volans.sink.combiner

import com.haizhi.volans.sink.config.constant.HiveStoreType
import com.haizhi.volans.sink.config.store.StoreHiveConfig
import com.haizhi.volans.sink.server.HiveDao
import com.haizhi.volans.sink.util.HDFSUtils
import com.haizhi.volans.sink.utils.DateUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.{AlreadyExistsException, Table}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
 * Author pengxb
 * Date 2020/11/26
 */
object CommitFileJob extends Runnable {

  private val logger = LoggerFactory.getLogger(getClass)
  private var storeConfig: StoreHiveConfig = _
  private var hiveDao: HiveDao = _
  private var table: Table = _

  // Job间隔时间(jobInterval)，默认1分钟
  private var jobInterval: Long = 60 * 1000
  // 表数据存储类型
  private var storeType: String = "text"

  /**
   * 初始化
   *
   * @param storeConfig
   */
  def init(storeConfig: StoreHiveConfig): Unit = {
    this.hiveDao = new HiveDao()
    this.storeConfig = storeConfig
    this.table = hiveDao.getTable(storeConfig.database, storeConfig.table)
    if (storeConfig.rollingPolicy.rolloverCheckInterval > 0) {
      this.jobInterval = storeConfig.rollingPolicy.rolloverCheckInterval * 1000
    }
    this.storeType = HiveStoreType.getStoreType(table.getSd.getInputFormat)
  }

  override def run(): Unit = {
    // 表数据存储路径
    val tableLocation = table.getSd.getLocation
    logger.info(s"表存储路径：$tableLocation")
    // 表分区信息
    val partitionKeys = hiveDao.getPartitionKeys(table)
    val combinerVo = storeConfig.rollingPolicy

    while (true) {
      try {
        if(!HDFSUtils.exists(combinerVo.workDir)){
          HDFSUtils.mkdirs(combinerVo.workDir)
        }
        // 处理分区文件
        if (partitionKeys.length > 0) {
          // 查找分区目录(包含文件且不包含子目录)
          val matchedPartitionDirList = HDFSUtils.listDirectoryWithoutSubDir(new Path(combinerVo.workDir), partitionKeys)
          logger.info(s"获取分区目录: $matchedPartitionDirList")
          println(s"获取分区目录: $matchedPartitionDirList")
          matchedPartitionDirList.foreach(partitionDir => {
            // 获取待处理文件列表
            val processingFileList = HDFSUtils.listFilesWithPattern(partitionDir, "(?!\\.complete$)")
            if (processingFileList != null && processingFileList.size > 0) {
              val dirName = processingFileList.head.getParent.toString
              val partitionLocPostFix = dirName.substring(combinerVo.workDir.length)
              val partitionLocation = tableLocation + partitionLocPostFix

              /**
               * 获取分区value列表, Example: /year=2020/month=11 => [2020,11]
               */
              val partitionValues = partitionLocPostFix
                .substring(1)
                .split(java.io.File.separator)
                .map(_.split("=")(1))
                .toList
                .asJava
              // 如果分区HDFS路径不存在，则创建分区
              if (!HDFSUtils.exists(partitionLocation)) {
                try {
                  logger.info(s"分区路径[$partitionLocation]不存在，创建分区")
                  hiveDao.addPartition(table, partitionValues, partitionLocPostFix)
                } catch {
                  case e: AlreadyExistsException => logger.warn(s"分区[$partitionLocPostFix]已经存在")
                }
              }
              // 再次判断HDFS分区目录是否存在，处理hive metastore不一致情况
              if (!HDFSUtils.exists(partitionLocation)) {
                HDFSUtils.mkdirs(partitionLocation)
              }
              processingFileList.foreach(srcFile => {
                val fmtTime = DateUtils.formatCurrentDate("yyyyMMddHHmmss")
                val destFile = new StringBuilder()
                  .append(partitionLocation)
                  .append(java.io.File.separator)
                  .append(srcFile.getName)
                  .append("-")
                  .append(fmtTime)
                  .append(".")
                  .append(storeType).toString
                // 移动文件到表分区目录
                logger.info(s"移动文件到分区目录：源文件[$srcFile]，目标文件[$destFile]")
                HDFSUtils.rename(srcFile, new Path(destFile))
              })
            }
          })
        } else {
          // 处理非分区文件
          logger.info("处理非分区文件")
          // 获取待处理文件列表
          val processingFileList = HDFSUtils.listFilesWithPattern(combinerVo.workDir, "(?!\\.complete$)")
          logger.info(s"待处理文件列表: $processingFileList")
          processingFileList.foreach(srcFile => {
            val fmtTime = DateUtils.formatCurrentDate("yyyyMMddHHmmss")
            val destFile = new StringBuilder()
              .append(tableLocation)
              .append(java.io.File.separator)
              .append(srcFile.getName)
              .append("-")
              .append(fmtTime)
              .append(".")
              .append(storeType).toString
            // 移动文件到表存储目录
            logger.info(s"移动文件到表目录，源文件[$srcFile]，目标文件[$destFile]")
            HDFSUtils.rename(srcFile, new Path(destFile))
          })
        }
        Thread.sleep(jobInterval)
      } catch {
        case e: Exception =>
          logger.error(e.getMessage, e)
          e.printStackTrace()
      }
    }
  }
}
