/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.util

import com.KanariDigital.app.AppContext
import com.KanariDigital.app.AppConfig

import com.KanariDigital.Orderbook.HdfsHiveWriter.OrderBookContext
import com.kanaridi.dsql.FlowSpecParse

/** util for converting context objects
  */
object ContextUtils {

  /** convert orderbook context to app context */
  def orderBook2AppContext(appName: String, orderBookContext: OrderBookContext): AppContext = {

    val messageMapper = orderBookContext.xform

    val hbaseConfig = orderBookContext.hbaseConfiguration
    val obAppConfig = orderBookContext.obAppConfig
    val hdfsAuditLogConfig = orderBookContext.hdfsAuditLoggerConfig

    var appConfig = new AppConfig

    appConfig.appName = appName

    //app
    val appConfigMap: Map[String, String] = Map(
      "enable-dedup" -> obAppConfig.enableDedup.getOrElse(""),
      "dedup-colfamily" -> obAppConfig.dedupColFamily.getOrElse(""),
      "dedup-colname" -> obAppConfig.dedupColName.getOrElse(""),
      "dedup-lookback" -> obAppConfig.dedupLookback.getOrElse(""),
      "max-batch-size" -> obAppConfig.maxBatchSize.getOrElse(""),
      "default-storage-level" -> obAppConfig.defaultStorageLevel.getOrElse(""))

    val auditConfigMap: Map[String, String] = Map(
      "path" -> hdfsAuditLogConfig.path,
      "filename-prefix" -> hdfsAuditLogConfig.fileNamePrefix)

    //hbase
    val hbaseConfigMap: Map[String, String] = Map(
      "zookeeper.quorum" -> hbaseConfig.quorum.getOrElse(""),
      "zookeeper.client.port" -> hbaseConfig.zookeeperClientPort.getOrElse("").toString,
      "kerberos.principal" -> hbaseConfig.kerberosPrincipal.getOrElse(""),
      "kerberos.keytab" -> hbaseConfig.kerberosKeytab.getOrElse(""),
      "distributed-mode" -> hbaseConfig.distrubutedMode.getOrElse("").toString,
      "scanner-caching" -> hbaseConfig.scannerCaching.getOrElse("").toString
    )

    //set app, hbase, audit log
    appConfig.appConfigMap = appConfigMap
    appConfig.hbaseConfigMap = hbaseConfigMap
    appConfig.auditLogConfigMap = auditConfigMap

    //set the appConfig
    new AppContext(appConfig, messageMapper, FlowSpecParse("{}"))
  }
}
