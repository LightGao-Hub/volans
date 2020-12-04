package com.haizhi.volans.sink.combiner

import java.util.Date

import com.haizhi.volans.sink.config.constant.HiveStoreType
import com.haizhi.volans.sink.config.store.StoreHiveConfig
import com.haizhi.volans.sink.server.HiveDao
import com.haizhi.volans.sink.util.{HDFSUtils, HiveUtils}
import com.haizhi.volans.sink.utils.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.metastore.api.{AlreadyExistsException, Partition, Table}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Author pengxb
 * Date 2020/11/2
 *
 * 一：正常文件合并流程
 * 1.查找待处理文件列表toDoFileList("part-"开头, 非.merging/.merged结尾)
 * 2.根据合并规则，从toDoFileList计算得到待合并文件列表mergingFileList
 * 3.将待合并文件列表(mergingFileList)中的文件重命名为part-xxxx.merging，开始文件合并操作，
 * 合并中的目标文件名称为".merging-时间戳.文件类型(textFile/parquet/orc)"
 * 4.文件合并完成后得到已合并文件列表mergedFileList，接下来：
 * 4.1 待合并文件列表中的文件重命名为"part-xxxx.merged"，
 * 4.2 已合并文件列表中的文件重命名为"merged-时间戳.文件类型"
 * 4.3 删除待合并文件列表文件
 *
 * 二：Job第一次启动或中断后的恢复流程
 * 不同场景处理方案如下：
 * 1.上次Job还未完成文件合并
 * 将上次合并中的文件".merged-时间戳.文件类型"删除，重新合并待合并文件列表中的文件(part-xxxx.merging)
 * 2.上次Job已经完成文件合并
 * 执行"正常文件合并流程"中的4.2、4.3步骤
 *
 */
class CombineFileJobV2(var hiveDao: HiveDao, var storeConfig: StoreHiveConfig) {

  private val logger = LoggerFactory.getLogger(getClass)
  private var table: Table = _
  private var combiner: Combiner = _
  // 文件状态
  private val FILESTATE_MERGING = ".merging"
  private val FILESTATE_MERGED = ".merged"
  // 工作目录
  private var workDir: String = _
  // Job首次启动标识
  private var isFirstLaunch = true
  // Job间隔时间(jobInterval)，默认1分钟
  private var jobInterval: Long = 60 * 1000
  // 分区缓存
  private var partitionCacheMap: java.util.Map[String, String] = _

  /**
   * 初始化
   *
   */
  def init(): Unit = {
    this.table = hiveDao.getTable(storeConfig.database, storeConfig.table)
    this.workDir = table.getSd.getLocation
    // 构建combiner
    combiner = buildCombiner(HiveUtils.getTableStoredType(table))
    if (storeConfig.rollingPolicy.rolloverCheckInterval > 0) {
      this.jobInterval = storeConfig.rollingPolicy.rolloverCheckInterval * 1000
    }
  }

  /**
   * 执行文件合并、提交分区操作
   */
  def runTask(): Unit = {
    try {
      // 表数据存储路径
      val tableLocation = HiveUtils.getTableLocation(table)
      logger.info(s"表存储路径：$tableLocation")
      // 表分区信息
      val partitionKeys = HiveUtils.getPartitionKeys(table)
      val combinerVo = storeConfig.rollingPolicy
      val workSpace = new Path(workDir)

      /**
       * 第一次启动时，处理上次程序退出未完成合并的文件
       */
      if (isFirstLaunch) {
        logger.info(s"首次启动Job，开始处理上次未完成的文件...")
        if (partitionKeys.length > 0) {
          // 缓存分区信息
          cachePartitionInfo()

          // 查找分区目录(包含文件且不包含子目录)
          val matchedPartitionDirList = HDFSUtils.listDirectoryWithoutSubDir(workSpace, partitionKeys)
          matchedPartitionDirList.foreach(partitionDir => {
            if (combinerVo.rolloverEnable) {
              // 处理未合并的文件
              handleUnsolvedFile(partitionDir)
            }
            // 刷新分区
            refreshPartition(tableLocation, partitionDir.toString)
          })
        } else {
          if (combinerVo.rolloverEnable) {
            handleUnsolvedFile(workSpace)
          }
        }
      }

      // 处理分区文件
      if (partitionKeys.length > 0) {
        // 查找分区目录(包含文件且不包含子目录)
        val matchedPartitionDirList = HDFSUtils.listDirectoryWithoutSubDir(workSpace, partitionKeys)
        logger.info(s"获取分区目录: $matchedPartitionDirList")
        matchedPartitionDirList.foreach(partitionDir => {
          if (combinerVo.rolloverEnable) {
            val mergedFileList = combineFile(storeConfig, combiner, partitionDir)
            logger.info(s"分区目录[$partitionDir]，已合并文件列表[$mergedFileList]")
          }
          // 刷新分区
          refreshPartition(tableLocation, partitionDir.toString)
        })
      } else {
        if (combinerVo.rolloverEnable) {
          logger.info("处理非分区文件")
          combineFile(storeConfig, combiner, workSpace)
        }
      }
      // 修改首次处理任务的标识
      isFirstLaunch = false
    } catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
        e.printStackTrace()
      }
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
   */
  def handleUnsolvedFile(workDir: Path): Unit = {
    // 查找part-xxx.merging文件
    val mergingFileList: List[Path] = HDFSUtils.listFileStatusByFilter(workDir, (fileName: String) => {
      fileName.startsWith("part-") && fileName.endsWith(FILESTATE_MERGING)
    }).map(_.getPath)
    // 查找part-xxx.merged文件
    val mergedFileList: List[Path] = HDFSUtils.listFileStatusByFilter(workDir, (fileName: String) => {
      fileName.startsWith("part-") && fileName.endsWith(FILESTATE_MERGED)
    }).map(_.getPath)
    // 查找".merging"开头文件
    val destMergingFileList: List[Path] = HDFSUtils.listFileStatusByFilter(workDir, (fileName: String) => {
      fileName.startsWith(FILESTATE_MERGING)
    }).map(_.getPath)

    /**
     * 极端情况：文件合并完成，但待合并文件(mergingFileList)在重命名过程中部分成功，部分失败
     * Example: part-xxx.merging => part-xxx.merged
     */
    if (mergingFileList != null && mergingFileList.size > 0 && mergedFileList != null && mergedFileList.size > 0) {
      // 继续将mergingFileList中的文件重命名
      val _mergedFileList = mergingFileList.map(srcFile => {
        val destFile = new Path(
          new StringBuilder()
            .append(srcFile.toString.substring(0, srcFile.toString.length - FILESTATE_MERGING.length))
            .append(FILESTATE_MERGED)
            .toString
        )
        HDFSUtils.rename(srcFile, destFile)
        destFile
      })
      // 将".merging"开头文件重命名为"merged"
      if (destMergingFileList != null && destMergingFileList.size > 0) {
        destMergingFileList.foreach(srcFile => {
          val destFile = new Path(
            new StringBuilder()
              .append(srcFile.getParent)
              .append(java.io.File.separator)
              .append(FILESTATE_MERGED.substring(1))
              .append(srcFile.getName.substring(FILESTATE_MERGING.length))
              .toString
          )
          HDFSUtils.rename(srcFile, destFile)
        })
      }
      // 删除待合并文件列表
      HDFSUtils.deleteFiles(_mergedFileList)
      HDFSUtils.deleteFiles(mergedFileList)
    } else {
      // 上次Job存在未完成的待合并文件
      if (mergingFileList != null && mergingFileList.size > 0) {
        /**
         * 检查是否存在.merging开头文件，存在则删除
         * Example: .merging-20201201100000.parquet
         */
        if (destMergingFileList != null && destMergingFileList.size > 0) {
          HDFSUtils.deleteFiles(destMergingFileList)
        }
        // 重新执行合并操作
        mergeFile(mergingFileList, combiner)
      }

      // 上次Job已完成文件合并操作，但未清理待合并文件列表
      if (mergedFileList != null && mergedFileList.size > 0) {
        /**
         * 如果存在以".merging"开头的文件，则将其重命名为".merged"
         * Example: .merging-20201201100000.parquet => merged-20201201100000.parquet
         */
        HDFSUtils.listFileStatusByFilter(workDir, (fileName: String) => {
          fileName.startsWith(FILESTATE_MERGING)
        }).foreach(fileStatus => {
          val srcFile = fileStatus.getPath()
          HDFSUtils.rename(srcFile,
            new Path(
              new StringBuilder()
                .append(srcFile.getParent)
                .append(java.io.File.separator)
                .append(FILESTATE_MERGED.substring(1))
                .append(srcFile.getName.substring(FILESTATE_MERGING.length))
                .toString
            )
          )
        })
        // 删除待合并文件列表
        HDFSUtils.deleteFiles(mergedFileList)
      }
    }
  }

  /**
   * 合并文件操作
   *
   * @param mergingFileList 待合并文件列表
   * @param combiner        文件合并器
   * @return destFile
   */
  def mergeFile(mergingFileList: List[Path], combiner: Combiner): Path = {
    // 待合并文件名称添加标识".merging"
    var _mergingFileList = HDFSUtils.addPostFixToPath(mergingFileList, FILESTATE_MERGING)
    val fmtTime = DateUtils.formatCurrentDate("yyyyMMddHHmmss")

    val dirName = _mergingFileList.head.getParent.toString
    /**
     * 待合并文件名
     * Example: ${workDir}/.merging-20201105160000.parquet
     */
    val mergingFile = new Path(
      new StringBuilder()
        .append(dirName)
        .append(java.io.File.separator)
        .append(FILESTATE_MERGING)
        .append("-")
        .append(fmtTime)
        .append(".")
        .append(combiner.getStoreType())
        .toString()
    )
    /**
     * 更新合并后的文件名
     * Example: ${workDir}/merged-20201105160000.parquet
     */
    val mergedFile = new Path(
      new StringBuilder()
        .append(dirName)
        .append(java.io.File.separator)
        .append(FILESTATE_MERGED.substring(1))
        .append("-")
        .append(fmtTime)
        .append(".")
        .append(combiner.getStoreType())
        .toString()
    )
    logger.info(s"目标文件合并中，路径[$mergingFile]")
    // 合并文件
    combiner.combineFile(mergingFile, _mergingFileList)

    // 待合并文件列表中的文件名后缀修改为".merged"
    _mergingFileList = _mergingFileList.map(srcFile => {
      val destFile = new Path(
        new StringBuffer()
          .append(srcFile.toString.substring(0, srcFile.toString.length - FILESTATE_MERGING.length))
          .append(FILESTATE_MERGED)
          .toString
      )
      HDFSUtils.rename(srcFile, destFile)
      destFile
    })

    // 将已合并的目标文件重命名(如果此时执行Hive查询会存在重复数据）
    HDFSUtils.rename(mergingFile, mergedFile)
    logger.info(s"目标文件合并完成，路径[$mergedFile]")

    // 删除待合并文件
    HDFSUtils.deleteFiles(_mergingFileList)
    logger.info(s"删除待合并文件: ${_mergingFileList}")
    mergedFile
  }

  /**
   * 根据文件合并规则合并文件
   *
   * @param hiveConfig hiveConfig
   * @param combiner   文件合并器
   * @param targetDir  目标文件目录
   * @return
   */
  def combineFile(hiveConfig: StoreHiveConfig, combiner: Combiner, targetDir: Path): List[Path] = {
    val combinerVo = hiveConfig.rollingPolicy

    // 自定义过滤函数，获取待处理文件列表(part-开头，非.merging/.merged结尾)
    def filterFunc: String => Boolean = (fileName: String) =>
      fileName.startsWith("part-") && !fileName.endsWith(FILESTATE_MERGING) && !fileName.endsWith(FILESTATE_MERGED)

    // val todoFileList = HDFSUtils.listFileStatusWithPattern(targetDir, Pattern.compile("^part\\-.*(?!\\.merging)|(?!\\.merged)$"))
    val todoFileList = HDFSUtils.listFileStatusByFilter(targetDir, filterFunc)
    logger.info(s"待处理文件列表：$todoFileList")

    // 检查目标文件存储格式合法性
    combiner.checkFileStoreType(todoFileList.map(_.getPath))

    // 待合并文件列表
    val mergingFileList = new ListBuffer[FileStatus]
    // 已合并文件列表
    val mergedFileList = new ListBuffer[Path]

    // 按文件修改时间排序，旧文件放前面
    var sortedFileStatusList = new ListBuffer[FileStatus] ++=
      todoFileList.sortWith(_.getModificationTime < _.getModificationTime)
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
          val mergedFile = mergeFile(mergingList, combiner)
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
          val mergedFile = mergeFile(mergingList, combiner)
          mergedFileList.append(mergedFile)
          tempMergingList.clear()
        }
      }
      if (tempMergingList.size > 0) {
        val mergingList = tempMergingList.toList
        val mergedFile = mergeFile(mergingList, combiner)
        mergedFileList.append(mergedFile)
      }
      sortedFileStatusList = sortedFileStatusList.filter(currentTimeStamp - _.getModificationTime < rollInterval)
    }

    mergedFileList.toList
  }

  /**
   * 刷新分区
   *
   * @param tableLocation     表存储路径
   * @param partitionLocation 分区目录路径
   * @return
   */
  def refreshPartition(tableLocation: String, partitionLocation: String): Unit = {
    // 提取分区信息
    val Tuple2(partitionLocPostFix: String, partitionValues: java.util.List[String]) =
      extractPartitionInfo(tableLocation, partitionLocation)
    var existsPartition: Boolean = false
    // 获取分区过滤表达式
    val filterExpr = HiveUtils.getPartitionFilterExpr(table, partitionValues)
    if (StringUtils.isNotBlank(filterExpr)) {
      // 获取表分区
      val partitionList: java.util.List[Partition] = hiveDao.listPartitionByFilter(table, filterExpr, Short.MaxValue)
      // 判断分区是否存在
      existsPartition = HiveUtils.existsPartition(partitionValues, partitionList)
    }
    logger.info(s"分区[$partitionLocation]是否存在: $existsPartition")
    if (!existsPartition) {
      try {
        logger.info(s"尝试创建分区[$partitionLocation]...")
        hiveDao.addPartition(table, partitionValues, partitionLocPostFix)
        logger.info("分区创建成功")
      } catch {
        case e: AlreadyExistsException => logger.warn(s"分区[$partitionLocPostFix]已经存在")
        case e1: Exception => logger.error(e1.getMessage, e1)
      }
    }
  }

  /**
   * 提取分区信息
   *
   * @param tableLocation     表存储路径
   * @param partitionLocation 分区目录路径
   * @return
   */
  def extractPartitionInfo(tableLocation: String, partitionLocation: String): (String, java.util.List[String]) = {
    val partitionLocPostFix = partitionLocation.substring(tableLocation.length)
    /**
     * 获取分区value列表, Example: /year=2020/month=11 => [2020,11]
     */
    val partitionValues = partitionLocPostFix
      .substring(1)
      .split("/")
      .map(_.split("=")(1))
      .toList
      .asJava
    (partitionLocPostFix, partitionValues)
  }

  /**
   *
   * 缓存分区信息
   */
  def cachePartitionInfo(): Unit = {

  }

}