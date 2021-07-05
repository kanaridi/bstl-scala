/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook.HdfsHiveWriter
import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import com.KanariDigital.JsonConfiguration.JsonConfig
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.{Success, Try}
import scala.collection.mutable.{Map => MutableMap}


class HdfsHiveWriterConfig(hiveTableMap : Option[Map[String,String]]) extends Serializable {
  var hdfsRaw : Option[String] = None
  var saveMode : Option[String] = None
  var enabled = false
  protected val hiveTable : Option[Map[String,String]] = hiveTableMap
  def hiveTopicMapping(topic : String) : Option[String] = {
    if (hiveTable.isDefined) hiveTable.get.get(topic) else None
  }
  override def toString() : String = {
    val undef = "Undefined"
    var raw = hdfsRaw.getOrElse(undef)
    val save = saveMode.getOrElse(undef)
    val enabledStr = this.enabled.toString
    val theMap = if (hiveTable.isDefined) hiveTable.get.map(x => x._1 + "->" + x._2).mkString(", ") else undef
    s"HDFS Hive Writer Config = [Enabled=$enabledStr raw=$raw, save-mode=$save hive-table: $theMap"
  }
}




object HdfsHiveWriterConfig {
  def readJsonFile(inputStream: InputStream): Try[HdfsHiveWriterConfig] = {
    val jsonConfig = new JsonConfig
     jsonConfig.read(inputStream, Some("hdfs-write"))

    val hive = jsonConfig.getJsonObject("hive_mapping")
    var mm = MutableMap[String, String]()

    if (hive.isDefined) {
      var iter = hive.get.keySet().iterator()
      while (iter.hasNext()) {
        val k = iter.next()
        if (k.isInstanceOf[String]) {
          val v = hive.get.get(k)
          if (v.isInstanceOf[String])
            mm += k.asInstanceOf[String] -> v.asInstanceOf[String]
        }
      }
    }
    val config = new HdfsHiveWriterConfig(if (hive.isDefined) Some(mm.toMap) else None)

    config.hdfsRaw = jsonConfig.getStringValue("hdfs-raw")
    config.saveMode = jsonConfig.getStringValue("save-mode")
    config.enabled = {
      val enabledValue = jsonConfig.getStringValue("enabled")
      enabledValue.isDefined && enabledValue.get.toLowerCase() == "true"
    }
    Success(config)
  }

  def readJsonFile(path: String, config: Configuration): Try[HdfsHiveWriterConfig] = {
    val log: Log = LogFactory.getLog(classOf[HdfsHiveWriterConfig].getName)
    val fileSystem = FileSystem.get(config)
    val inputStream = fileSystem.open(new Path(path))
    val configResult = readJsonFile(inputStream)
    inputStream.close()
    configResult
  }

}
