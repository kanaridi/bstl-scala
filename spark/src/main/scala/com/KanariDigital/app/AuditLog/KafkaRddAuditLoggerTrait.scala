/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.AuditLog

import org.apache.spark.streaming.kafka010.OffsetRange

trait KafkaRddAuditLoggerTrait {
  def logKafkaOffsetRanges(uow : UnitOfWork, offsetRanges : Seq[OffsetRange])
  def insertLogMessage(message : String)
  def flush() : Unit
}
