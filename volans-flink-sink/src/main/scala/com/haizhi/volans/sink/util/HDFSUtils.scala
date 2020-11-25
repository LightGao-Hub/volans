package com.haizhi.volans.sink.util

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
 * Author pengxb
 * Date 2020/11/3
 */
object HDFSUtils {

  private val logger = LoggerFactory.getLogger(getClass)
  private var fs = FileSystem.get(new Configuration())
  //  private var fs: FileSystem = _

  def init(conf: Configuration): Unit = {
    this.fs = FileSystem.get(conf)
  }

  def readFully(path: String): String = {
    val p = new Path(path)
    val sb = new StringBuilder()
    try {
      if (!fs.exists(p) || fs.getFileStatus(p).isDirectory) {
        return sb.result()
      }
      val input = fs.open(p)
      val inr = new InputStreamReader(input)
      val read = new BufferedReader(inr)
      var line = "";
      while (line != null) {
        line = read.readLine()
        if (line != null) {
          sb.append(line)
        }
      }
    } catch {
      case e: IOException => logger.error(e.getMessage,e)
    }
    sb.result()
  }

  def upload(content: String, path: String): Unit = {
    import java.io._
    import java.util._
    logger.info(s"content=${content}")
    val workDirPath = System.getProperty("user.dir")
    val workDir = new File(workDirPath)
    val file = new File(workDir, new Date().getTime.toString)
    val runFileWrite = new FileWriter(file)
    runFileWrite.write(content)
    runFileWrite.close()
    fs.copyFromLocalFile(true, new Path(file.getAbsolutePath), new Path(path))
  }

  def moveFile(srcPath: Path, destDir: String): Unit = {
    moveFile(srcPath, new Path(destDir))
  }

  /**
   * 给文件名添加后缀
   */
  def addPostFixToPath(srcPath: Path, postFix: String): Unit ={
    rename(srcPath,new Path(srcPath +"/" + postFix))
  }

  def rename(srcFile: Path, descFile: Path): Unit ={
    fs.rename(srcFile,descFile)
  }

  def rename(srcFile: String, descFile: String): Unit ={
    rename(new Path(srcFile),new Path(descFile))
  }

  def moveFile(srcPath: Path, destDir: Path): Unit = {
    val destPath = new Path(destDir + "/" + srcPath.getName)
    fs.rename(srcPath, destPath)
  }

  def moveFiles(srcPaths: List[Path], destDir: String): Unit = {
    for (srcPath <- srcPaths) {
      val destPath = new Path(destDir + "/" + srcPath.getName)
      fs.rename(srcPath, destPath)
    }
  }

  def deleteDirectory(targetDir: Path,recursive: Boolean): Unit ={
      fs.delete(targetDir,recursive)
  }

  def deleteDirectory(targetDir: String,recursive: Boolean): Unit ={
    fs.delete(new Path(targetDir),recursive)
  }

  def deleteDirectoryList(targetDirList: List[Path],recursive: Boolean): Unit ={
    for(path <- targetDirList){
      fs.delete(path,recursive)
    }
  }

  def deleteFiles(pathList: List[Path]): Unit ={
    for(path <- pathList){
      fs.deleteOnExit(path)
    }
  }

  def deleteFile(file: Path): Unit ={
      fs.deleteOnExit(file)
  }

  /**
   * 批量合并文件
   */
  def mergeFilesBath(srcPaths: List[Path], destFile: Path): Unit = {
    if (!fs.exists(destFile)) {
      fs.createNewFile(destFile)
    }
    val os = fs.create(destFile)
    for (srcPath <- srcPaths) {
      val buffer = new Array[Byte](8 * 1024 * 1024)
      val is = fs.open(srcPath)
      var len = is.read(buffer)
      while (len != -1) {
        os.write(buffer, 0, len - 1)
        len = is.read(buffer)
      }
      os.flush()
      is.close()
    }
    os.close()
  }

  /**
   * 根据过滤条件列出目录下的文件
   */
  def listFilesWithPattern(targetDir: Path, filterPattern: String): List[Path] = {
    val pattern = Pattern.compile(filterPattern)
    val remoteFileIter = fs.listFiles(targetDir, false)
    val matchPathList = new collection.mutable.ListBuffer[Path]()
    while (remoteFileIter.hasNext) {
      val fileStatus = remoteFileIter.next()
      val matcher = pattern.matcher(fileStatus.getPath.getName)
      if (matcher.find()) {
        matchPathList.append(fileStatus.getPath)
      }
    }
    matchPathList.toList
  }

  def listFilesWithPattern(targetDir: String, filterPattern: String): List[Path] = {
    listFilesWithPattern(new Path(targetDir), filterPattern)
  }

  def listFileStatus(pathList: Array[Path]): Array[FileStatus] = {
    fs.listStatus(pathList)
  }

  def listFileStatus(targetDir: String): Array[FileStatus] ={
    this.listFileStatus(new Path(targetDir))
  }

  def listFileStatus(targetDir: Path): Array[FileStatus] ={
    fs.listStatus(targetDir)
  }

  /**
   * 列举包含文件但不包含子目录的目录
   * @param dir 目标目录
   * @param pathFilterList 目录名称过滤列表，如List("year","month","day")
   * @return
   */
  def listDirectoryWithoutSubDir(dir: Path, pathFilterList: List[String]): List[Path] ={
    val fileStatusList = fs.listStatus(dir,new PathFilter {
      override def accept(path: Path): Boolean = {
        !pathFilterList.find(pattern => path.toString.contains(pattern)).isEmpty
      }
    })
    val buffer = new ListBuffer[Path]
    var fileCount = 0
    for(i <- 0 until fileStatusList.length){
      if(fileStatusList(i).isDirectory){
        buffer ++= listDirectoryWithoutSubDir(fileStatusList(i).getPath,pathFilterList)
      }else{
        fileCount += 1
      }
    }
    if(fileStatusList.length > 0 && fileCount == fileStatusList.length){
      buffer += dir
    }
    buffer.toList
  }

  def exists(path: String): Boolean = {
    fs.exists(new Path(path))
  }

  def exists(path: Path): Boolean = {
    fs.exists(path)
  }

  def isDirectory(path: String): Boolean = {
    fs.isDirectory(new Path(path))
  }

  def mkdirs(dir: String): Unit = {
    this.mkdirs(new Path(dir))
  }

  def mkdirs(dir: Path): Unit = {
    if (!fs.exists(dir)) {
      fs.mkdirs(dir)
    }
  }

  def close(): Unit = {
    try {
      if (this.fs != null) {
        this.fs.close()
      }
    } catch {
      case e: Exception => logger.error(e.getMessage,e)
    } finally {
      this.fs = null
    }
  }

}