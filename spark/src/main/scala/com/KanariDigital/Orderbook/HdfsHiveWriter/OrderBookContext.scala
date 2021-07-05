/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook.HdfsHiveWriter

import com.KanariDigital.app.AuditLog.{HdfsLogger, HdfsLoggerConfig}
import com.KanariDigital.hbase.HBaseConfiguration
import com.kanaridi.xform.MessageMapper
import com.KanariDigital.Orderbook.OBAppConfiguration

class OrderBookContext(val hbaseConfiguration: HBaseConfiguration,
  val hdfsHiveFactory : HdfsHiveWriterFactoryTrait,
  val hdfsAuditLoggerConfig: HdfsLoggerConfig,
  val errorLogger : HdfsLogger,
  val xform: MessageMapper,
  val obAppConfig: OBAppConfiguration) extends Serializable {
}
