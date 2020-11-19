package com.haizhi.volans.sink.combiner
import org.apache.hadoop.fs.Path

/**
 * Author pengxb
 * Date 2020/11/4
 */
class AvroFileCombiner(var storeType: String = "avro") extends Combiner {
  /**
   * 检查待合并文件的存储格式是否一致
   *
   * @param fileList
   * @return
   */
  override def checkFileStoreType(fileList: List[Path]): Boolean = ???

  /**
   * 合并文件
   *
   * @param destFile
   * @param mergeFileList
   */
  override def combineFile(destFile: Path, mergeFileList: List[Path]): Unit = ???

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

  override def getStoreType(): String = this.storeType

}
