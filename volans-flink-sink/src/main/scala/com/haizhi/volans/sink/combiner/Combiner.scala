package com.haizhi.volans.sink.combiner

import org.apache.hadoop.fs.Path

/**
 * Author pengxb
 * Date 2020/11/3
 */
trait Combiner {

  var storeType: String

  def getStoreType(): String

  /**
   * 检查待合并文件的存储格式是否一致
   *
   * @param fileList
   * @return
   */
  def checkFileStoreType(fileList: List[Path]): Boolean

  /**
   * 获取文件数据行数
   * @param filePath
   * @return
   */
  def getNumberOfRows(filePath : Path): Long

  /**
   * 获取文件列表行数
   * @param filePathList
   * @return
   */
  def getNumberOfRows(filePathList: List[Path]): Map[Path,Long]

  /**
   * 合并文件
   *
   * @param destFile
   * @param mergingFileList
   */
  def combineFile(destFile: Path, mergingFileList: List[Path])

}
