/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler

import com.kanaridi.common.util.HdfsUtils
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Storage extends Serializable {
  protected val log: Log = LogFactory.getLog(this.getClass.getName)
  var storagePath:String = ""
  var configPath:String = ""

  def setRootPath(path: String) {storagePath = path}
  def getRootPath(): String = storagePath

  /**
   * Write content to a subfolder inside the storage root path.
   * @param content
   * @param folder
   * @param fileName
   */
  def write(content: String, folder: String, fileName: String): Unit = {
    val fullPath = getRootPath() + folder + "/" + fileName
    log.info(s"write text content to $fullPath")
    HdfsUtils.saveFileContents(fullPath, ArrayBuffer(content))
  }

  /**
   * Write to root folder of the storage
   * @param content
   * @param fileName
   */
  def write(content: String, fileName: String): Unit = {
    val fullPath = getRootPath() + fileName
    log.info(s"write text content to $fullPath")
    HdfsUtils.saveFileContents(fullPath, ArrayBuffer(content))
  }

  /**
   * read the file from a specific folder of the storage
   * @param folder
   * @param fileName
   * @return
   */
  def read(folder: String, fileName: String): String = {
    val mockResult = StorageFactory.getMockData(fileName)
    if (mockResult.isDefined) {
      log.warn(s"Using Mock Data for Dictionary $folder/$fileName")
      return mockResult.get
    }
    val fullPath = getRootPath() + folder + "/" + fileName
    log.warn("FILE fullPath=" + fullPath)
    val buffer = HdfsUtils.readFileContents(new Path(fullPath))
    buffer.mkString("\n")
  }

  /**
   * Read the file from root folder of the storage
   * @param fileName
   * @return
   */
  def read(fileName: String): String = {
    val mockResult = StorageFactory.getMockData(fileName)
    if (mockResult.isDefined) {
      log.warn(s"Using Mock Data for Dictionary $fileName")
      return mockResult.get
    }
    val fullPath = getRootPath() + fileName
    val buffer = HdfsUtils.readFileContents(new Path(fullPath))
    buffer.mkString("\n")
  }

  /**
   * Copy all files from source location to root path
   * creates destination directory if it's missing
   * @param fromDir
   * @param toDir
   */
  def copyFiles(fromDir: String, toDir: String): Unit = {
    HdfsUtils.copyFiles(fromDir, toDir, "*", true)
  }
}

object StorageFactory {
  protected val log: Log = LogFactory.getLog(this.getClass.getName)
  var mapStorage: mutable.Map[String, Storage] = mutable.Map[String, Storage]()
  var mockDataMap: mutable.Map[String, String] = scala.collection.mutable.Map[String,String]()

  def get(pathOverride:String = ""): Storage = {
    val newStorage =
      if (mapStorage.contains(pathOverride)) {
        mapStorage(pathOverride)
      } else {
        mapStorage(pathOverride) = new Storage()
        mapStorage(pathOverride)
      }

    newStorage.setRootPath(pathOverride)
    newStorage
  }
  /**
   * Sets up mock data to run unit test
   * @param mockPath this is the path that the caller will query for
   * @param data the data contents they will receive instead of the actual file
   */
  def setMockData(mockPath: String, data: String) {mockDataMap.put(mockPath, data)}
  def clearMockData() {mockDataMap = mockDataMap.empty}
  def getMockData(path: String): Option[String] = {mockDataMap.get(path)}

}
