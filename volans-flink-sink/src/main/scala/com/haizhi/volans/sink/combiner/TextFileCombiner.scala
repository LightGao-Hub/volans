package com.haizhi.volans.sink.combiner

import com.haizhi.volans.sink.util.HDFSUtils
import org.apache.hadoop.fs.Path

/**
 * Author pengxb
 * Date 2020/11/3
 */
class TextFileCombiner(var storeType: String = "text") extends Combiner {

  /**
   * 检查待合并文件的存储格式是否一致 Todo
   * @param fileList
   * @return
   */
  override def checkFileStoreType(fileList: List[Path]): Boolean = {
    true
  }

  /**
   * 合并Text文件
   * @param destFile
   * @param mergingFileList
   */
  override def combineFile(destFile: Path, mergingFileList: List[Path]): Unit = {
    HDFSUtils.mergeFilesBath(mergingFileList,destFile)
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

  override def getStoreType(): String = this.storeType

}
