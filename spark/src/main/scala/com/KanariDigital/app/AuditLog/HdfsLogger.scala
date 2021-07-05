/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.AuditLog
import java.time.Instant

import com.KanariDigital.KafkaConsumer.{KafkaOperationFailure, KafkaOperationResult, KafkaOperationSuccessful, KafkaTopicLoggerTrait}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import scala.collection.mutable.ArrayBuffer

class HdfsLogger(
  hdfsFilepath: String,
  filenamePrefix: String,
  @transient val hadoopConfiguration : HadoopConfiguration)
    extends AbstractAuditLogger with KafkaTopicLoggerTrait {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  private var logMessages = ArrayBuffer.empty[String]

  private var filePath : Option[String] = None

  @transient lazy private val changedPerm: FsPermission =
    new FsPermission(FsAction.READ_WRITE, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE)

  private def preparePath(filePath: String) : Unit = {
    val prefix = filePath.substring(0, filePath.lastIndexOf("/"))
    val fileSystem = FileSystem.get(hadoopConfiguration)
    val path = new Path(prefix)

    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path,changedPerm)
    }
  }

  private def createFileNameFromTimeStamp() : Option[String] = {
    val currentSecs: Long = Instant.now().toEpochMilli
    Some(hdfsFilepath + "/" + filenamePrefix + "_"+ sdf.format(Instant.now().toEpochMilli).substring(0, 10)+".txt")
  }

  private def createHdfsStream(hdfsFilePath: String) : FSDataOutputStream = {
    val fileSystemInstance = FileSystem.get(hadoopConfiguration)
    val outputPath = new Path(hdfsFilePath)

    if (fileSystemInstance.exists(outputPath)) {
      fileSystemInstance.append(outputPath)
    } else {
      val str = fileSystemInstance.create(outputPath, true)
      fileSystemInstance.setPermission(outputPath, changedPerm)
      str
    }
  }

  override def insertLogMessage(message: String): Unit = {
    if (filePath.isDefined == false) {
      filePath = createFileNameFromTimeStamp()
    }
    logMessages += formattedLogLine(message + "\n")
  }

  // The flush just puts the output to the stream
  override def flush(): Unit = {
    if (filePath.isDefined) {
      log.info("Going to write to filePath: " + filePath.get)
      preparePath(filePath.get)
      val outputStream = createHdfsStream(filePath.get)
      logMessages.foreach(m => outputStream.write(m.getBytes()))
      outputStream.close()
      logMessages = ArrayBuffer.empty[String]
    }
  }

  def reset() : Unit = {
    flush()
    this.filePath = None
  }

  override def log(topic: String, kafkaOperationResult: KafkaOperationResult): Unit = {
    kafkaOperationResult match {
      case succ : KafkaOperationSuccessful => {
        // Log nothing when successful
      }
      case kafkaFailure : KafkaOperationFailure => {
        insertLogMessage(kafkaFailure.toString())
        reset()
      }
    }
  }


  def logException(exception : Exception) : Unit = {
    insertLogMessage(s"Exception: ${exception.toString}")
  }

  def logThrowableException(th : Throwable) : Unit = {
    insertLogMessage(s"Throwable: ${th.toString}")
  }
}


object HdfsLogger {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  def create(hdfsFilepath: String, filenamePrefix: String) : AbstractAuditLogger = {
    val hadoopConfiguration = new HadoopConfiguration()
    hadoopConfiguration.set("defaultfs", "hdfs:///")
    return new HdfsLogger(hdfsFilepath, filenamePrefix, hadoopConfiguration)
  }

}
