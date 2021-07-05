/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.AuditLog

import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ArrayBuffer

class CompositeAuditLogger extends AbstractAuditLogger {
  val log: Log = LogFactory.getLog(this.getClass.getName)

  var loggers = ArrayBuffer.empty[AbstractAuditLogger]

  def withLogger(logger : AbstractAuditLogger) : CompositeAuditLogger = {
    loggers += logger
    this
  }

  override def insertLogMessage(message: String): Unit = {
    loggers.foreach(_.insertLogMessage(message))
  }

  override def flush() : Unit = {
    loggers.foreach(_.flush())
  }
}

object CompositeAuditLogger {
  def createLogAuditLogger() : CompositeAuditLogger = {
    new CompositeAuditLogger()
      .withLogger(new Log4JAuditLogger())
  }

  def createHdfsAndLogAuditLogger(hdfsAuditLoggerConfig: HdfsLoggerConfig) : CompositeAuditLogger = {
    new CompositeAuditLogger()
      .withLogger(new Log4JAuditLogger())
      .withLogger(HdfsLogger.create(hdfsAuditLoggerConfig.path, hdfsAuditLoggerConfig.fileNamePrefix))
  }

}
