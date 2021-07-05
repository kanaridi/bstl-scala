/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.AuditLog

import java.io.InputStream

import com.KanariDigital.JsonConfiguration.JsonConfig
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.{Failure, Success, Try}

class HdfsLoggerConfig(val path : String, val fileNamePrefix : String) extends Serializable {
}

object HdfsLoggerConfig extends Serializable {
  val log = LogFactory.getLog(HdfsLoggerConfig.getClass.getName)

  def readJsonFile(inputStream: InputStream, configSection : String): Try[HdfsLoggerConfig] = {
    val jsonConfig = new JsonConfig
    jsonConfig.read(inputStream, Some(configSection))

    val path = jsonConfig.getStringValue("path")
    val prefix = jsonConfig.getStringValue("filename-prefix")
    if (path.isDefined) {
      log.info(s"Read log HDFS Logger config [path = ${path.get}, prefix = ${prefix.get}] ")
      Success(new HdfsLoggerConfig(path.get, prefix.get))
    } else {
      val message ="Unable to configure HDFS Logger"
      log.error(message)
      Failure(new RuntimeException(message))
    }
  }

  def readJsonFile(path: String, configSection : String, config: Configuration): Try[HdfsLoggerConfig] = {
    log.info(s"Reading AuditLog Configuration from $path")
    val fileSystem = FileSystem.get(config)
    val inputStream = fileSystem.open(new Path(path))
    val hdfsLoggerConfig = readJsonFile(inputStream, configSection)
    inputStream.close()
    hdfsLoggerConfig
  }
}
