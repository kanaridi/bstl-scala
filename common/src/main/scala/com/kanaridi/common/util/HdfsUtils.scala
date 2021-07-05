/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.common.util

import java.util.EnumSet

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Stack}
import scala.io.{BufferedSource, Source}
import scala.util.{Try, Success, Failure}

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Options.Rename
import org.apache.hadoop.fs.permission.FsPermission


/** util for interacting with the HDFS
  */
object HdfsUtils {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  var fc = FileContext.getFileContext()

  /** set up file system to store offsets, default is hdfs */
  def initLocalFileContext() = {
    fc = FileContext.getLocalFSFileContext()
  }

  def listDirectories(fromDir: String, filePattern: String = "*"): Array[FileStatus] = {
    val pathFilter = new GlobFilter(filePattern)
    val fromDirPath = new Path(fromDir)
    val directories = fc.util.listStatus(fromDirPath, pathFilter).filter(_.isDirectory)
    directories
  }

  /** list files from directory with given pattern
    * @param fromDir list files from directory
    * @param filePattern glob filter pattern for files
    * @return list of files in the directory sorted by filename
    */
  def listFiles(fromDir: String, filePattern: String, recursive: Boolean = false): Array[Path] = {

    //get latest file in the folder
    val pathFilter = new GlobFilter(filePattern)

    val fromDirPath = new Path(fromDir)

    if (recursive) {
      val dirs = Stack[String]()
      val files = ListBuffer.empty[Path]
      dirs.push(fromDir)
      while (!dirs.isEmpty) {
        val curPath = dirs.pop()
        val statusList = fc.util.listStatus(new Path(curPath), pathFilter)
        statusList.foreach(x => {
          if(x.isDirectory) {
            dirs.push(x.getPath.toString)
          } else {
            files += new Path(curPath, x.getPath.getName)
          }
        })
      }
      files.foreach(f => log.info(f.toString))
      files.toArray
    } else {
      val fileStatusList = fc.util.listStatus(fromDirPath, pathFilter).filter(_.isFile)
      val fileNameList = fileStatusList.map(fileStatus => {
        fileStatus.getPath.getName
      }).sorted.reverse

      val fileList = fileNameList.map(fileName => {
        new Path(fromDirPath, fileName)
      })
      fileList
    }
  }

  /** move files between directories, creates destination
    * directory if missing
    */
  def moveFiles(fromDir: String, toDir: String, filePattern: String): Unit = {

    // get a list of source files
    val sourceFiles = listFiles(fromDir, filePattern)

    val changedPerm: FsPermission = new FsPermission("777")

    // toDir path
    val toDirPath = new Path(toDir)

    // create toDir if necessary, create
    // any missing parent directories
    fc.mkdir(toDirPath, changedPerm, true)

    // move all the sourceFiles
    for (sourceFile <- sourceFiles) {
      val destFile = new Path(toDirPath, sourceFile.getName)
      log.info("HdfsUtils: rename: {" + sourceFile + "} to {" + destFile + "}")
      fc.rename(sourceFile, destFile, Rename.NONE)
      log.info("HdfsUtils: rename: {" + sourceFile + "} to {" + destFile + "}")
    }

  }

  /** copy files between directories, creates destination
    * directory if missing
    */
  def copyFiles(fromDir: String, toDir: String, filePattern: String, recursive: Boolean = false): Unit = {

    // get a list of source files
    val sourceFiles = listFiles(fromDir, filePattern, recursive)

    val changedPerm: FsPermission = new FsPermission("777")

    log.info(s"Copying ${sourceFiles.length} files")
    // move all the sourceFiles
    for (sourceFile <- sourceFiles) {
      // Generate the path for destination based on source path
      val index = sourceFile.toString.lastIndexOf(sourceFile.getName)
      val curSubPath = sourceFile.toString.substring(fromDir.length, index)
      var toDirPath: Path = null
      if (curSubPath.isEmpty) {
        toDirPath = new Path(toDir)
      } else {
        toDirPath = new Path(toDir, curSubPath)
      }

      if (!fc.util.exists(toDirPath)) {
        fc.mkdir(toDirPath, changedPerm, true)
      }

      val destFile = new Path(toDirPath, sourceFile.getName)
      log.info("HdfsUtils: copy: {" + sourceFile + "} to {" + destFile + "}")
      fc.util.copy(sourceFile, destFile, false, true)
    }
  }

  /** create a directory with read write permission */
  def createDirectory(directory: String) = {

    val changedPerm: FsPermission = new FsPermission("777")

    //get history
    fc.mkdir(
      new Path(directory),
      changedPerm,
      true)
  }

  /** move specified files to destination. Creates destination directory
    * if its missing
    * @param candidateFiles files to move
    * @param toDir destination directory
    */
  def moveFiles(candidateFiles: Array[Path], toDir: String): Unit = {

    // create directory
    this.createDirectory(toDir)

    for (candidateFile <- candidateFiles) {
      val destFile = new Path(toDir, candidateFile.getName)
      log.info("HdfsUtils: rename: {" + candidateFile + "} to {" + destFile + "}")
      fc.rename(candidateFile, destFile, Rename.NONE)
    }
  }

  /** move specified files to destination. Creates destination directory
    * if its missing
    * @param candidateFiles files to move
    * @param toDir destination directory
    */
  def moveFilesPreservePath(candidateFiles: Array[String], toDir: String): Unit = {

    for (candidateFile <- candidateFiles) {
      val sourceFile = new Path(candidateFile)
      val level2Dir = sourceFile.getParent
      val level1Dir = level2Dir.getParent
      val toDirPreservePath = toDir + "/" + level1Dir.getName + "/" + level2Dir.getName
      // create directory
      this.createDirectory(toDirPreservePath)
      val destFile = new Path(toDirPreservePath, sourceFile.getName)
      log.info("HdfsUtils: rename preserve path: {" + sourceFile + "} to {" + destFile + "}")
      fc.rename(sourceFile, destFile, Rename.NONE)
    }
  }

  /** move specified files to destination. Creates destination directory
    * if its missing
    * @param candidateFiles files to move
    * @param toDir destination directory
    */
  def moveFiles(candidateFiles: Array[String], toDir: String): Unit = {

    // create directory
    this.createDirectory(toDir)

    for (candidateFile <- candidateFiles) {
      val sourceFile = new Path(candidateFile)
      val destFile = new Path(toDir, sourceFile.getName)
      log.info("HdfsUtils: rename: {" + sourceFile + "} to {" + destFile + "}")
      fc.rename(sourceFile, destFile, Rename.NONE)
    }
  }


    /** save contents to a file
    */
  def saveFileContents(filePathStr: String, contents: ArrayBuffer[String]): Unit = {
    saveFileContents(filePathStr, contents.toList)
  }

  /** save contents to a file
    */
  def saveFileContents(filePathStr: String, contents: List[String]): Unit = {

    var outputStream: Option[FSDataOutputStream] = None

    try {

      val filePath = new Path(filePathStr)

      outputStream = Some(_createStream(filePath))

      //get the offset spec json to store
      contents.foreach(content => {
        outputStream.get.write((content + "\n").getBytes)
      })


    } catch {

      case ex: Throwable => {
          throw new Exception(ex.getMessage)
      }

    } finally {
      if (outputStream.isDefined) { outputStream.get.close }
    }
  }

  /** get fs output stream to store the offsets*/
  private def _createStream(filePath: Path): FSDataOutputStream = {

    val changedPerm: FsPermission = new FsPermission("777")
    //FsAction.READ and FsAction.WRITE and FsAction.EXECUTE)//o

    val outputStream = fc.create(
      filePath,
      EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
      Options.CreateOpts.createParent(),
      Options.CreateOpts.perms(changedPerm)
    )

    log.info("HdfsUtils: _createStream: created outputStream to write to: {" + filePath + "}")
    outputStream
  }

  /** get fs output stream to store the offsets*/
  private def _createAppendStream(filePath: Path): FSDataOutputStream = {

    val changedPerm: FsPermission = new FsPermission("777")
    //FsAction.READ and FsAction.WRITE and FsAction.EXECUTE)//o

    val outputStream = fc.create(
      filePath,
      EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND),
      Options.CreateOpts.createParent(),
      Options.CreateOpts.perms(changedPerm)
    )

    log.info("HdfsUtils: _createStream: created outputStream to write to: {" + filePath + "}")
    outputStream
  }

  /** Read file from hdfs
    *
    * @param filePath path to the file to read
    * @return lines from a file as {{ ArrayBuffer[String] }}
    */
  def readFileContents(filePath: Path): ArrayBuffer[String] = {

    var bufferedSource:Option[BufferedSource] = None

    try {

      var fileContents: ArrayBuffer[String] = ArrayBuffer.empty[String]

      bufferedSource = Some(Source.fromInputStream(fc.open(filePath),
        "UTF-8"))

      for (line <- bufferedSource.get.getLines) {
        fileContents += line
      }

      fileContents

    } catch {

      case ex: Throwable => {
        throw new Exception(ex.getMessage)
      }

    } finally {

      if (bufferedSource.isDefined) bufferedSource.get.close

    }
  }

  /** delete candidate files from directory */
  def removeFiles(candidateFiles: Array[Path], fromDir: String): Unit = {
    for (candidateFile <- candidateFiles) {
      removeFile(candidateFile, fromDir)
    }
  }

  /** delete candidate files from directory */
  def removeFile(candidateFile: Path, fromDir: String): Unit = {
    if (candidateFile.toString.startsWith(fromDir) && fc.util.exists(candidateFile)) {
      log.info("HdfsUtils: going to remove: {" + candidateFile + "} ...")
      val recurse = false
      fc.delete(candidateFile, recurse)
    }
  }

  /** delete specified empty files from a directory */
  def removeEmptyFiles(candidateFiles: Array[Path], fromDir: String): Unit = {
    for (candidateFile <- candidateFiles) {
      removeEmptyFile(candidateFile, fromDir)
    }
  }

  /** delete specified empty file from directory */
  def removeEmptyFile(candidateFile: Path, fromDir: String): Unit = {
    val fileSize = fc.getFileStatus(candidateFile).getLen
    if (candidateFile.toString.startsWith(fromDir) && fileSize == 0){
      log.info("HdfsUtils: going to remove empty file: {" + candidateFile + "} ...")
      val recurse = false
      fc.delete(candidateFile, recurse)
    }
  }

  /** concat source files to destination file */
  def concatFiles(destinationFile: Path, sourceFiles: ArrayBuffer[Path]): Unit = {

    var outputStream: Option[FSDataOutputStream] = None

    try {

      outputStream = Some(_createAppendStream(destinationFile))

      for (sourceFile <- sourceFiles) {

        var bufferedSource: Option[BufferedSource] = None

        try {

          bufferedSource = Some(Source.fromInputStream(
            fc.open(sourceFile),
            "UTF-8"))

          // copy from source, append to destination file
          for (line <- bufferedSource.get.getLines) {
            outputStream.get.write((line + "\n").getBytes)
          }

        } catch {

          case ex: Throwable => {
            throw new Exception(ex.getMessage)
          }

        } finally {
          if (bufferedSource.isDefined) bufferedSource.get.close
        }
      }

    } catch {

      case ex: Throwable => {
        throw new Exception(ex.getMessage)
      }

    } finally {
      if (outputStream.isDefined) outputStream.get.close
    }
  }

  /** get a temporary file  */
  def getTemporaryFile(scratchDir: String, appName: String, currentBatchMillis: Long): Option[Path] = {

    try {

      val appStr = if (appName.trim != "") appName else System.currentTimeMillis.toString
      val currentBatchStr = if (currentBatchMillis > 0) currentBatchMillis.toString else System.currentTimeMillis.toString
      val current = System.currentTimeMillis()

      val tempFile = (scratchDir: String, currentBatchStr: String, current: Long) => {
        if (scratchDir != "") {
          new Path(scratchDir, s"${appStr}_${currentBatchStr}_${current}.txt")
        } else {
          new Path(fc.getHomeDirectory, s"${appStr}_${currentBatchStr}_${current}.txt")
        }
      }

      Some(tempFile(scratchDir, currentBatchStr, current))

    } catch {
      case ex: Throwable => {
        // ex.printStackTrace
        None
      }
    }
  }

  /** get a iterator to go over directories */
  def getDirectoryIterator(incomingDir: String): Option[RemoteIterator[LocatedFileStatus]] = {

    try {

      // get an iterator and start processing files in depth first order
      val filesIter: RemoteIterator[LocatedFileStatus] = fc.util.listFiles(new Path(incomingDir), true)

      Some(filesIter)

    } catch {
      case ex: Throwable => {
        // ex.printStackTrace
        None
      }
    }
  }

  /** Get a path to application directory.
    * If the path is relative path (not preceded by a /) then
    * construct a path from users home directory, otherwise just return
    * absolute path
    */
  def getAppDir(dirPath: String, appUniqueStr: String): Option[Path] = {

    try {

      val appLogDir = (dirPath: String, appStr: String) => {
        if (dirPath.startsWith("/")) {
          new Path(dirPath)
        } else {
          val appDir = if (appStr != "") new Path(fc.getHomeDirectory, appStr) else fc.getHomeDirectory
          if (dirPath != "") {
            new Path(appDir, dirPath)
          } else {
            appDir
          }
        }
      }

      Some(appLogDir(dirPath, appUniqueStr))

    } catch {
      case ex: Throwable => {
        // ex.printStackTrace
        None
      }
    }
  }

  /** get file status given a path string
    */
  def getFileStatus(pathStr: String): Try[FileStatus] = {
    val path = new Path (pathStr)
    getFileStatus(path)
  }

  /** get file status given a path
    */
  def getFileStatus(path: Path): Try[FileStatus] = Try {
    fc.getFileStatus(path)
  }

  /**
    * resolve path components into a absolute path
    */
  def resolvePaths(base: String, pathComponents: String*): Path =  {
    val basePath = new Path(base)
    resolvePaths(basePath, pathComponents: _*)
  }

  /**
    * resolve path components into a absolute path
    */
  def resolvePaths(basePath: Path, pathComponents: String*): Path =  {
    pathComponents.foldLeft(basePath){(retPath: Path, pathComp: String) =>
      new Path(retPath, new Path(pathComp))
    }
  }
}
