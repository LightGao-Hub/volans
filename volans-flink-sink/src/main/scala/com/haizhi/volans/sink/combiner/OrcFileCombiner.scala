package com.haizhi.volans.sink.combiner

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcFile, Writer}

/**
 * Author pengxb
 * Date 2020/11/3
 */
class OrcFileCombiner(var storeType: String = "orc") extends Combiner {

  /**
   * 检查待合并文件的存储格式是否一致 Todo
   * @param fileList
   * @return
   */
  override def checkFileStoreType(fileList: List[Path]): Boolean = {
    true
  }

  /**
   * 合并Orc文件
   *
   * @param destFile
   * @param mergingFileList
   */
  override def combineFile(destFile: Path, mergingFileList: List[Path]): Unit = {
    val conf = new Configuration()
    var reader = OrcFile.createReader(mergingFileList.head, OrcFile.readerOptions(conf))
    val schema = reader.getSchema
    val writer: Writer = OrcFile.createWriter(destFile, OrcFile.writerOptions(conf).setSchema(schema))
    for(mergingFile <- mergingFileList){
      reader = OrcFile.createReader(mergingFile, OrcFile.readerOptions(conf))
      val batch = reader.getSchema.createRowBatch()
      val rows = reader.rows()
      while(rows.nextBatch(batch)){
        if(batch != null){
          writer.addRowBatch(batch)
        }
      }
      rows.close()
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

  override def getStoreType(): String = this.storeType
}
