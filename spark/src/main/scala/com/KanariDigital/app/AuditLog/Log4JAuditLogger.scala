/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.AuditLog

import org.apache.commons.logging.{Log, LogFactory}

class Log4JAuditLogger extends AbstractAuditLogger {
  val log: Log = LogFactory.getLog(this.getClass.getName)

  override def insertLogMessage(message: String): Unit = {
    log.info(formattedLogLine(message))
  }

  override def flush(): Unit = {}
}
