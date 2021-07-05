/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.AuditLog
import java.time.Instant

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


trait HeartBeatStatusEmitter {
  def enable(seconds : Long) : Unit
  def signalAlive() : Boolean
  def disable() : Unit
}

class HeartBeatLogger(filePath : String) extends Serializable with HeartBeatStatusEmitter {
  var enabled = false
  var lastLogged = 0L
  var heartBeatFrequencyMiliseconds = 0L
  @transient lazy val configuration = createConfiguration()
  var firstCall = true

  private def createConfiguration() : Configuration = {
    val conf = new Configuration()
    conf.set("defaultfs", "hdfs:///")
    conf
  }

  override def enable(seconds : Long) : Unit = {
    enabled = true
    heartBeatFrequencyMiliseconds = seconds * 1000L
    signalAlive()
  }

  override def disable() : Unit = {
    enabled = false
    lastLogged = 0L
  }

  private def touchFile() : Unit = {
    val fileSystemInstance = FileSystem.get(configuration)
    // The first time through ensure that the path is created
    if (firstCall) {
      val prefix = filePath.substring(0, filePath.lastIndexOf("/"))
      val path = new Path(prefix)
      fileSystemInstance.mkdirs(path)
      firstCall = false
    }
    val str = fileSystemInstance.create(new Path(filePath), true)
    str.close()
  }

  override def signalAlive(): Boolean = {
    val now = Instant.now().toEpochMilli
    val delta = now - lastLogged
    if (delta > heartBeatFrequencyMiliseconds) {
      touchFile()
      lastLogged = now
      true
    } else
      false
  }
}

