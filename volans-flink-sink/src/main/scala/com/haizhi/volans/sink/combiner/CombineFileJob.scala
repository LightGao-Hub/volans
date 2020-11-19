package com.haizhi.volans.sink.combiner

import java.util.Date

import com.haizhi.volans.sink.config.store.StoreHiveConfig
import com.haizhi.volans.sink.config.constant.HiveStoreType
import com.haizhi.volans.sink.server.HiveDao
import com.haizhi.volans.sink.util.HDFSUtils
import com.haizhi.volans.sink.utils.DateUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
 * Author pengxb
 * Date 2020/11/2
 */
object CombineFileJob extends Thread {

  private val logger = LoggerFactory.getLogger(getClass)
  private var configList: mutable.Set[(StoreHiveConfig, Boolean)] = _
  private val hiveDao = new HiveDao()


  def init(configList: mutable.Set[StoreHiveConfig]): Unit = {
    this.configList = configList.map((_, true))
  }

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
    val fileList = fileStatusList.filter(_.isFile)
    val dirList = fileStatusList.filter(_.isDirectory)
    var mergingFileList = new ListBuffer[Path]
    for (dir <- dirList) {
      val fmtTime = dir.getPath.getName.replaceAll(".*stage-", "")
      val filterList = fileList.filter(_.getPath.getName.contains(fmtTime))
      if (filterList.size > 0) {
        //已合并但未移动到Hive仓库的文件
        //删除stage目录
        HDFSUtils.deleteDirectory(dir.getPath, true)
      } else {
        val fileList = HDFSUtils.listFileStatus(dir.getPath)
        val mergingFile = fileList.filter(_.getPath.getName.endsWith(".merging")).head
        //上一个stage未完成合并文件操作，需要删除.merging文件并重跑任务
        if (mergingFile != null) {
          HDFSUtils.deleteFile(mergingFile.getPath)
        }
        mergingFileList ++= fileList.filter(!_.getPath.getName.endsWith(".merging")).map(_.getPath)
      }
    }
    val mergedFileList = new ListBuffer[Path] ++= fileList.map(_.getPath)
    if (mergingFileList.size > 0) {
      //合并文件
      mergedFileList += mergeFile(mergingFileList.toList, workDir, combiner, isPartitioned)
      //删除临时文件
      HDFSUtils.deleteDirectory(mergingFileList.head.getParent, true)
    }
    mergedFileList.toList
  }

  /**
   * 合并文件
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
    //临时目录
    val tmpDir = workDir + "/.working"
    HDFSUtils.mkdirs(tmpDir)

    var stageTaskDir = tmpDir + "/stage-" + fmtTime
    var partitionName = ""
    if (isPartitoned) {
      val dirName = mergingFileList.head.getParent.toString
      partitionName = dirName.substring(dirName.lastIndexOf(workDir) + 1).split("/").mkString("_") + "-"
      stageTaskDir = tmpDir + "/" + partitionName + "stage-" + fmtTime
      // Example: ${workDir}/.working/year=2020_month=11_day=05-stage-20201105160000
    }
    HDFSUtils.mkdirs(stageTaskDir)
    //移动待合并文件到stage目录
    HDFSUtils.moveFiles(mergingFileList, stageTaskDir)
    val _mergingFileList = mergingFileList.map(path => new Path(stageTaskDir + "/" + path.getName))

    var preFix = new StringBuilder()
      .append(stageTaskDir).append("/")
      .append(partitionName)
      .append("part-")
      .append(fmtTime)
      .append(".")
      .append(combiner.getStoreType())
      .toString()
    var postFix = ".merging"
    // Example: ${workDir}/.working/stage-20201105160000/part-20201105160000.parquet.merging
    val mergingFile = new Path(preFix + postFix)
    //合并文件
    combiner.combineFile(mergingFile, _mergingFileList)

    preFix = new StringBuilder().append(tmpDir)
      .append("/")
      .append(partitionName)
      .append("part-")
      .append(fmtTime)
      .append(".")
      .append(combiner.getStoreType())
      .toString()
    postFix = ".merged"
    // Example: ${tempDir}/.working/part-20201105160000.parquet.merged
    val mergedFile = new Path(preFix + postFix)
    //更新合并后的文件名
    HDFSUtils.rename(mergingFile, mergedFile)
    //删除待合并文件和stage目录
    HDFSUtils.deleteDirectory(stageTaskDir, true)
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
    //获取目标文件列表
    val pathList = HDFSUtils.listFilesWithPattern(targetDir, "(?!\\.complete$)")
    //检查目标文件存储格式合法性
    combiner.checkFileStoreType(pathList)

    //获取文件信息
    val fileStatusList = HDFSUtils.listFileStatus(pathList.toArray)
    //待合并文件列表
    val mergingFileList = new ListBuffer[FileStatus]
    //已合并文件列表
    val mergedFileList = new ListBuffer[Path]

    //按文件修改时间排序，旧文件放前面
    var sortedFileStatusList = new ListBuffer[FileStatus] ++=
      fileStatusList.sortWith(_.getModificationTime < _.getModificationTime)
    /**
     * 根据rollSize合并文件
     */
    var rollSizeCount = 0L
    val rollSize = combinerVo.maxPartSize
    if (rollSize > 0) {
      for (fileStatus <- sortedFileStatusList) {
        mergingFileList.append(fileStatus)
        rollSizeCount += fileStatus.getLen
        //文件达到rollSize大小，开始滚动文件
        if (rollSizeCount >= rollSize) {
          val mergingList = mergingFileList.map(_.getPath).toList
          val mergedFile = mergeFile(mergingList, combinerVo.workDir, combiner, isPartitioned)
          mergedFileList.append(mergedFile)
          //重置条件
          rollSizeCount = 0L
          mergingFileList.clear()
        }
      }
      sortedFileStatusList = mergingFileList
    }

    /**
     * 根据rollInterval处理剩余待合并的文件
     */
    val rollInterval = combinerVo.rolloverInterval * 60 * 1000
    if (rollInterval > 0) {
      val batchSize = 1024
      var num = 0L
      val currentTimeStamp = new Date().getTime
      //过滤得到待合并文件列表
      val mergingFileList = sortedFileStatusList.filter(fileStatus => {
        //文件修改时间与系统时间间隔
        (currentTimeStamp - fileStatus.getModificationTime) >= rollInterval
      })
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
    while (true) {
      try {
        for (configTuple <- configList) {
          val hiveConfig: StoreHiveConfig = configTuple._1
          val table = hiveDao.getTable(hiveConfig.database, hiveConfig.table)
          //表数据存储类型
          val storeType = HiveStoreType.getStoreType(table.getSd.getInputFormat)
          //表数据存储路径
          val tableLocation = table.getSd.getLocation
          //表分区信息
          val partitionKeys = hiveDao.getPartitionKeys(table)
          val partitionKeyList = partitionKeys.asJava
          //构建combiner
          val combiner = buildCombiner(storeType)
          val combinerVo = hiveConfig.rollingPolicy
          if (configTuple._2) {
            /**
             * 第一次启动时，处理上次程序退出未完成的文件
             */
            val isPartitioned = partitionKeys.length > 0
            val unSolvedMergedList = handleUnsolvedFile(combinerVo.workDir, combiner, isPartitioned)
            if (isPartitioned) {
              for (unSolvedMergedFile <- unSolvedMergedList) {
                val partitionName = unSolvedMergedFile.getName.split("-part-")(0).replaceAll("_", "/")
                val partitionLocation = tableLocation + "/" + partitionName
                //检查分区HDFS路径是否存在
                if (!HDFSUtils.exists(partitionLocation)) {
                  try {
                    //创建分区
                    hiveDao.addPartition(table, partitionKeyList, "/" + partitionName)
                  } catch {
                    case e: AlreadyExistsException => logger.warn(s"partition [/${partitionName}] already exists")
                  }
                }
                //移动文件到分区目录
                HDFSUtils.moveFile(unSolvedMergedFile, tableLocation)
              }
            } else {
              HDFSUtils.moveFiles(unSolvedMergedList, tableLocation)
            }
          }

          //处理分区文件
          if (partitionKeys.length > 0) {
            //查找包含文件但不包含子目录的分区目录
            val matchedPartitionDirList = HDFSUtils.listDirectoryWithoutSubDir(new Path(combinerVo.workDir), partitionKeys.toList)
            matchedPartitionDirList.map(targetDir => {
              (targetDir, combineFile(hiveConfig, combiner, targetDir, true))
            }).foreach(record => {
              val targetDir = record._1
              val _mergedFileList = record._2
              val partitionLocPostFix = targetDir.toString.substring(targetDir.toString.lastIndexOf(tableLocation) + 1)
              val partitionLocation = tableLocation + partitionLocPostFix
              //检查分区HDFS路径是否存在
              if (!HDFSUtils.exists(partitionLocation)) {
                try {
                  //创建分区
                  hiveDao.addPartition(table, partitionKeyList, partitionLocPostFix)
                } catch {
                  case e: AlreadyExistsException => logger.warn(s"partition [${partitionLocPostFix}] already exists")
                }
              }
              //移动文件到分区目录
              HDFSUtils.moveFiles(_mergedFileList, tableLocation)
            })
          } else {
            //处理非分区文件
            val mergedFileList = combineFile(hiveConfig, combiner, new Path(combinerVo.workDir), false)
            //移动合并后的文件到目标表仓库路径下
            if (mergedFileList.size > 0) {
              HDFSUtils.moveFiles(mergedFileList, tableLocation)
            }
          }
        }
        //修改首次处理任务的标识
        configList = configList.map(record => (record._1, false))
        //Job间隔时间(jobInterval)，默认5分钟
        Thread.sleep(5 * 60 * 1000)
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
        }
      }
    }
  }
}