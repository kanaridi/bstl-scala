/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.AuditLog
import java.text.SimpleDateFormat
import java.time.Instant

import org.apache.spark.streaming.kafka010.OffsetRange

abstract class AbstractAuditLogger extends Serializable with KafkaRddAuditLoggerTrait  {
  protected val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  private def currentTimeStamp() : String = {
    sdf.format(Instant.now().toEpochMilli).substring(0, 14)
  }

  protected def formattedLogLine(message: String) : String = {
    currentTimeStamp() + ": " + message
  }

  def logOffset(uow : UnitOfWork, offsetRange : OffsetRange): Unit = {
    insertLogMessage(s"[${uow.toString()}] Topic: ${offsetRange.topic}, Partition: ${offsetRange.partition}, Offset: [${offsetRange.fromOffset},${offsetRange.untilOffset}], Count: ${offsetRange.count()}")
  }

  override def logKafkaOffsetRanges(uow : UnitOfWork, offsetRanges: Seq[OffsetRange]): Unit = {
    offsetRanges.filter(o => o.count() > 0).foreach(o => logOffset(uow, o))
  }

  def flush() : Unit
}

