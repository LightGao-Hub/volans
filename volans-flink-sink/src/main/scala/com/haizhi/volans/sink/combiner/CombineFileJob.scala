package com.haizhi.volans.sink.combiner

import java.util.Date

import com.haizhi.volans.sink.config.store.StoreHiveConfig
import com.haizhi.volans.sink.config.constant.HiveStoreType
import com.haizhi.volans.sink.server.HiveDao
import com.haizhi.volans.sink.util.HDFSUtils
import com.haizhi.volans.sink.utils.DateUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.metastore.api.{AlreadyExistsException, Table}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
 * Author pengxb
 * Date 2020/11/2
 */
object CombineFileJob extends Thread {

  private val logger = LoggerFactory.getLogger(getClass)
  private var storeConfig: StoreHiveConfig = _
  // Job首次启动标识
  private var firstLaunchFlag = true

  private var hiveDao: HiveDao = _
  private var table: Table = _
  private var combiner: Combiner = _

  // 文件状态
  private val STATE_MERGING = ".merging"
  private val STATE_MERGED = ".merged"

  // Job间隔时间(jobInterval)，默认1分钟
  private var jobInterval: Long = 60 * 1000

  /**
   * 初始化
   *
   * @param storeConfig
   */
  def init(storeConfig: StoreHiveConfig): Unit = {
    this.hiveDao = new HiveDao()
    this.storeConfig = storeConfig
    this.table = hiveDao.getTable(storeConfig.database, storeConfig.table)
    // 表数据存储类型
    val storeType = HiveStoreType.getStoreType(table.getSd.getInputFormat)
    // 构建combiner
    combiner = buildCombiner(storeType)
    if (storeConfig.rollingPolicy.rolloverCheckInterval > 0) {
      this.jobInterval = storeConfig.rollingPolicy.rolloverCheckInterval * 1000
    }
  }

  /**
   * 根据表存储类型构建对应combiner
   *
   * @param storeType
   * @return
   */
  def buildCombiner(storeType: String): Combiner = {
    storeType match {
      case HiveStoreType.PARQUET => new ParquetFileCombiner()
      case HiveStoreType.ORC => new OrcFileCombiner()
      case HiveStoreType.TEXTFILE => new TextFileCombiner()
      case HiveStoreType.AVRO => new AvroFileCombiner()
      case HiveStoreType.RCFILE => new RcFileCombiner()
      case _ => {
        throw new RuntimeException("unsupported Hive Store Type: " + storeType)
      }
    }
  }

  /**
   * 处理上次程序退出时未处理完的文件
   *
   * @param workDir
   * @param combiner
   * @return
   */
  def handleUnsolvedFile(workDir: String, combiner: Combiner, isPartitioned: Boolean): List[Path] = {
    val tempDir = workDir + "/.working"
    val fileStatusList = HDFSUtils.listFileStatus(tempDir)
    // 文件列表
    val fileList = fileStatusList.filter(_.isFile)
    // 目录列表
    val dirList = fileStatusList.filter(_.isDirectory)
    // 待合并文件列表
    var mergingFileList = new ListBuffer[Path]
    for (dir <- dirList) {
      // 获取文件时间戳
      val fmtTime = dir.getPath.getName.replaceAll(".*stage-", "")
      val filterList = fileList.filter(_.getPath.getName.contains(fmtTime))
      if (filterList.size > 0) {
        // 已合并但未移动到Hive仓库目录的文件，删除stage目录
        HDFSUtils.deleteDirectory(dir.getPath, true)
      } else {
        val fileList = HDFSUtils.listFileStatus(dir.getPath)
        val mergingFile = fileList.filter(_.getPath.getName.endsWith(STATE_MERGING)).head
        // 上一个stage未完成合并文件操作，需要删除.merging文件并重跑任务
        if (mergingFile != null) {
          HDFSUtils.deleteFile(mergingFile.getPath)
        }
        mergingFileList ++= fileList.filter(!_.getPath.getName.endsWith(STATE_MERGING)).map(_.getPath)
      }
    }
    val mergedFileList = new ListBuffer[Path] ++= fileList.map(_.getPath)
    if (mergingFileList.size > 0) {
      // 合并文件
      mergedFileList += mergeFile(mergingFileList.toList, workDir, combiner, isPartitioned)
      // 删除临时文件
      HDFSUtils.deleteDirectory(mergingFileList.head.getParent, true)
    }
    mergedFileList.toList
  }

  /**
   * 合并文件操作
   *
   * @param mergingFileList 待合并文件列表
   * @param workDir         工作路径
   * @param combiner        combiner
   * @param isPartitoned    分区表标识
   * @return destFile
   */
  def mergeFile(mergingFileList: List[Path], workDir: String, combiner: Combiner, isPartitoned: Boolean): Path = {
    val fmtTime = DateUtils.formatCurrentDate("yyyyMMddHHmmss")
    /**
     * 每一批文件合并任务目录(stage)
     * Example: ${workDir}/.working/stage-20201105160000
     */
    // 临时目录
    val tmpDir = workDir + "/.working"
    HDFSUtils.mkdirs(tmpDir)

    var stageTaskDir = tmpDir + "/stage-" + fmtTime
    var partitionName = ""
    if (isPartitoned) {
      val dirName = mergingFileList.head.getParent.toString
      partitionName = dirName.substring(workDir.length + 1).split("/").mkString("_") + "-"
      stageTaskDir = tmpDir + "/" + partitionName + "stage-" + fmtTime
      // Example: ${workDir}/.working/year=2020_month=11_day=05-stage-20201105160000
    }
    logger.info(s"创建stage目录：$stageTaskDir")
    if (!HDFSUtils.exists(stageTaskDir)) {
      HDFSUtils.mkdirs(stageTaskDir)
    }
    // 移动待合并文件到stage目录
    HDFSUtils.moveFiles(mergingFileList, stageTaskDir)
    logger.info(s"移动待合并文件到stage目录")
    val _mergingFileList = mergingFileList.map(path => new Path(stageTaskDir + "/" + path.getName))

    var preFix = new StringBuilder()
      .append(stageTaskDir).append("/")
      .append(partitionName)
      .append("part-")
      .append(fmtTime)
      .append(".")
      .append(combiner.getStoreType())
      .toString()
    var postFix = STATE_MERGING
    // Example: ${workDir}/.working/stage-20201105160000/part-20201105160000.parquet.merging
    val mergingFile = new Path(preFix + postFix)
    logger.info(s"目标文件合并中，路径[$mergingFile]")
    // 合并文件
    combiner.combineFile(mergingFile, _mergingFileList)

    preFix = new StringBuilder().append(tmpDir)
      .append(java.io.File.separator)
      .append(partitionName)
      .append("part-")
      .append(fmtTime)
      .append(".")
      .append(combiner.getStoreType())
      .toString()
    postFix = STATE_MERGED
    // Example: ${tempDir}/.working/part-20201105160000.parquet.merged
    val mergedFile = new Path(preFix + postFix)
    // 更新合并后的文件名
    HDFSUtils.rename(mergingFile, mergedFile)
    logger.info(s"目标文件合并完成，路径[$mergedFile]")
    // 删除待合并文件和stage目录
    HDFSUtils.deleteDirectory(stageTaskDir, true)
    logger.info(s"删除待合并文件和stage目录")
    mergedFile
  }

  /**
   * 根据文件合并规则合并文件
   *
   * @param hiveConfig
   * @param combiner
   * @param targetDir
   * @param isPartitioned
   * @return
   */
  def combineFile(hiveConfig: StoreHiveConfig, combiner: Combiner, targetDir: Path, isPartitioned: Boolean): List[Path] = {
    val combinerVo = hiveConfig.rollingPolicy
    // 获取待处理文件列表
    val pathList = HDFSUtils.listFilesWithPattern(targetDir, "(?!\\.complete$)")
    logger.info(s"待处理文件列表：$pathList")
    // 检查目标文件存储格式合法性
    combiner.checkFileStoreType(pathList)

    // 获取文件信息
    val fileStatusList = HDFSUtils.listFileStatus(pathList.toArray)
    // 待合并文件列表
    val mergingFileList = new ListBuffer[FileStatus]
    // 已合并文件列表
    val mergedFileList = new ListBuffer[Path]

    // 按文件修改时间排序，旧文件放前面
    var sortedFileStatusList = new ListBuffer[FileStatus] ++=
      fileStatusList.sortWith(_.getModificationTime < _.getModificationTime)
    logger.info(s"sortedFileStatusList: $sortedFileStatusList")

    /**
     * 根据rollSize合并文件
     */
    var rollSizeCount = 0L
    val rollSize = combinerVo.maxPartSize
    if (rollSize > 0) {
      logger.info(s"根据rollSize[$rollSize]处理文件")
      for (fileStatus <- sortedFileStatusList) {
        mergingFileList.append(fileStatus)
        rollSizeCount += fileStatus.getLen
        // 文件达到rollSize大小，开始滚动文件
        if (rollSizeCount >= rollSize) {
          logger.info(s"文件大小超过$rollSize，开始合并文件...")
          val mergingList = mergingFileList.map(_.getPath).toList
          logger.info(s"待合并文件列表: $mergingFileList")
          val mergedFile = mergeFile(mergingList, combinerVo.workDir, combiner, isPartitioned)
          mergedFileList.append(mergedFile)
          // 重置条件
          rollSizeCount = 0L
          mergingFileList.clear()
        }
      }
      sortedFileStatusList = mergingFileList
    }

    /**
     * 根据rollInterval和inactivityInterval处理剩余待合并的文件
     */
    val rollInterval = combinerVo.rolloverInterval * 1000
    val inactivityInterval = combinerVo.inactivityInterval * 1000
    if (rollInterval > 0) {
      logger.info(s"根据rollInterval[$rollInterval], inactivityInterval[$inactivityInterval]处理剩余待合并文件")
      val batchSize = 1024
      var num = 0L
      val currentTimeStamp = new Date().getTime
      // 过滤得到待合并文件列表
      val mergingFileList = sortedFileStatusList.filter(fileStatus => {
        // 文件修改时间与系统时间间隔
        val modifyTimeDiff = currentTimeStamp - fileStatus.getModificationTime
        // 文件访问时间与系统时间间隔
        val accessTimeDiff = currentTimeStamp - fileStatus.getAccessTime
        modifyTimeDiff >= rollInterval || accessTimeDiff >= inactivityInterval
      })
      logger.info(s"待合并文件列表: $mergingFileList")
      val tempMergingList = new ListBuffer[Path]
      for (fileStatus <- mergingFileList) {
        tempMergingList.append(fileStatus.getPath)
        num += 1
        if (num % batchSize == 0) {
          val mergingList = tempMergingList.toList
          val mergedFile = mergeFile(mergingList, combinerVo.workDir, combiner, isPartitioned)
          mergedFileList.append(mergedFile)
          tempMergingList.clear()
        }
      }
      if (tempMergingList.size > 0) {
        val mergingList = tempMergingList.toList
        val mergedFile = mergeFile(mergingList, combinerVo.workDir, combiner, isPartitioned)
        mergedFileList.append(mergedFile)
      }
      sortedFileStatusList = sortedFileStatusList.filter(currentTimeStamp - _.getModificationTime < rollInterval)
    }

    mergedFileList.toList
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
        /**
         * 第一次启动时，处理上次程序退出未完成的文件
         */
        if (firstLaunchFlag) {
          logger.info("首次启动Job，开始处理上次未完成的文件...")
          println("首次启动Job，开始处理上次未完成的文件...")
          if (!HDFSUtils.exists(combinerVo.workDir)) {
            HDFSUtils.mkdirs(combinerVo.workDir)
          }
          // 分区标识
          val isPartitioned = partitionKeys.length > 0
          val tempWorkDir = combinerVo.workDir + "/.working"
          // 存在临时工作目录
          if (HDFSUtils.exists(tempWorkDir)) {
            // 获取上次未处理的文件列表
            val unSolvedMergedList = handleUnsolvedFile(combinerVo.workDir, combiner, isPartitioned)
            if (isPartitioned) {
              for (unSolvedMergedFile <- unSolvedMergedList) {
                val partitionName = unSolvedMergedFile.getName.split("-part-")(0).replaceAll("_", "/")
                val partitionLocation = tableLocation + "/" + partitionName
                val partitionValues = partitionName
                  .split(java.io.File.separator)
                  .map(_.split("=")(1))
                  .toList
                  .asJava
                // 检查分区HDFS路径是否存在
                if (!HDFSUtils.exists(partitionLocation)) {
                  try {
                    // 创建分区
                    hiveDao.addPartition(table, partitionValues, "/" + partitionName)
                  } catch {
                    case e: AlreadyExistsException => logger.warn(s"分区 [/${partitionName}]已经存在")
                  }
                }
                // 移动文件到分区目录
                HDFSUtils.moveFile(unSolvedMergedFile, tableLocation)
              }
            } else {
              HDFSUtils.moveFiles(unSolvedMergedList, tableLocation)
            }
          }
        }

        // 处理分区文件
        if (partitionKeys.length > 0) {
          // 查找分区目录(包含文件且不包含子目录)
          val matchedPartitionDirList = HDFSUtils.listDirectoryWithoutSubDir(new Path(combinerVo.workDir), partitionKeys)
          logger.info(s"获取分区目录: $matchedPartitionDirList")
          matchedPartitionDirList.map(targetDir => {
            (targetDir, combineFile(storeConfig, combiner, targetDir, true))
          }).foreach(record => {
            val targetDir = record._1
            val _mergedFileList = record._2
            logger.info(s"分区目录[$targetDir]，已合并文件列表[${_mergedFileList}]")
            if (_mergedFileList != null && _mergedFileList.size > 0) {
              val partitionLocPostFix = targetDir.toString.substring(tableLocation.length)
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
              // 移动文件到分区目录
              logger.info("移动文件到分区目录")
              HDFSUtils.moveFiles(_mergedFileList, partitionLocation)
            }
          })
        } else {
          logger.info("处理非分区文件")
          val mergedFileList = combineFile(storeConfig, combiner, new Path(combinerVo.workDir), false)
          // 移动合并后的文件到目标表仓库路径下
          if (mergedFileList.size > 0) {
            logger.info(s"移动文件到表存储目录[$tableLocation]")
            HDFSUtils.moveFiles(mergedFileList, tableLocation)
          }
        }
        // 修改首次处理任务的标识
        firstLaunchFlag = false
        Thread.sleep(jobInterval)
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
        }
      }
    }
  }
}