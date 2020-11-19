package com.haizhi.volans.sink.combiner

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.example.{GroupReadSupport, GroupWriteSupport}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader, ParquetWriter}
import org.slf4j.LoggerFactory

/**
 * Author pengxb
 * Date 2020/11/3
 */
class ParquetFileCombiner(var storeType: String = "parquet") extends Combiner {
//var rollSize:Long, var rollCount: Long, var rollInterval: Long
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * 检查待合并文件的存储格式是否一致 Todo
   * @param fileList
   * @return
   */
  override def checkFileStoreType(fileList: List[Path]): Boolean = {
    true
  }

  /**
   * 合并Parquet文件
   * @param destFile
   * @param mergingFileList
   */
  override def combineFile(destFile: Path, mergingFileList: List[Path]): Unit = {
//    val destFilePath = mergeDir + "/" + DateUtils.formatCurrentDate("yyyyMMddHHmmss") + ".complete"
    val conf = new Configuration()
    val metaData = ParquetFileReader.readFooter(conf, mergingFileList.head)
    val schema = metaData.getFileMetaData.getSchema
    GroupWriteSupport.setSchema(schema, conf)
    val writeSupport = new GroupWriteSupport()
    val writer = new ParquetWriter[Group](destFile, conf, writeSupport)
    var reader: ParquetReader[Group] = null
    var group: Group = null
    for (mergeFile <- mergingFileList) {
      reader = ParquetReader.builder(new GroupReadSupport(), mergeFile).build()
      group = reader.read()
      while (group != null) {
        writer.write(group)
        group = reader.read()
      }
      reader.close()
    }
    writer.close()
  }

  /**
   * 获取文件数据行数
   *
   * @param filePath
   * @return
   */
  override def getNumberOfRows(filePath: Path): Long = ???

  /**
   * 获取文件列表行数
   *
   * @param filePathList
   * @return
   */
  override def getNumberOfRows(filePathList: List[Path]): Map[Path, Long] = ???

  override def getStoreType(): String = storeType
}