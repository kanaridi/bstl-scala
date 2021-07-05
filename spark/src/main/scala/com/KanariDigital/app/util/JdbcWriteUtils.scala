/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.util

import com.KanariDigital.app.AppContext
import com.kanaridi.xform.HBRow
import com.KanariDigital.jdbc.{JdbcManager, jdbcColumnValue}

import scala.collection.mutable.{ArrayBuffer => MutableArrayBuffer}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.util.{Failure, Success}

object JdbcWriteUtils extends Serializable {
  val log: Log = LogFactory.getLog(this.getClass.getName)

  def writeResultsToJdbc(appContext: AppContext,
    batchTime: Long, hbrows: RDD[HBRow]): Long = {
    val appConfig = appContext.appConfig
    val applicationId = appConfig.getApplicationId
    val appName = appConfig.appName
    val appTypeStr = appConfig.appType
    val runtimeApplicationId = appConfig.runtimeAppId

    // jdbs connection properties
    val jdbcConfigMap = appConfig.jdbcConfigMap
    val sqlEnabledStr = jdbcConfigMap.getOrElse("enabled", "false")
    val sqlDriver = jdbcConfigMap.getOrElse("driver", "")
    val url = jdbcConfigMap.getOrElse("url", "")
    val dbName = jdbcConfigMap.getOrElse("database", "")
    val username = jdbcConfigMap.getOrElse("username", "")
    val password = jdbcConfigMap.getOrElse("password", "")
    val table = jdbcConfigMap.getOrElse("table", "")
    val batchSize = jdbcConfigMap.getOrElse("batch-size", "30000").toLong

    // app properties
    val appConfigMap = appConfig.appConfigMap
    val enableDedup = appConfigMap.getOrElse("enable-dedup", "false")
    val dedupColFamily = appConfigMap.getOrElse("dedup-colfamily", "d")
    val dedupColName = appConfigMap.getOrElse("dedup-colname", "dedup_hash")
    val dedupLookback = appConfigMap.getOrElse("dedup-lookback", "0")
    val maxBatchSize = appConfigMap.getOrElse("max-batch-size", "1000")

    var totalWrite = 0L

    if (sqlEnabledStr == "true") {
      log.info(s"$appName Jdbc write enabled is true: Writing JSON to sql...")
      val count = hbrows.count()
      log.info(s"hbrows has $count rows ")
      val start = System.currentTimeMillis

      val tableCountSet: RDD[(String, Int)] = hbrows.mapPartitionsWithIndex((partIndex, recIter) => {
        log.info(s"RDD_TRACE $batchTime: Entering mapPartitions for jdbc write...")
        var tableCount = MutableArrayBuffer.empty[Tuple2[String, Int]]
        if (recIter.hasNext) {
          val jdbcMgr = JdbcManager.create(
              sqlDriver, url, dbName,
              username, password,
              batchSize)


          //TODO: Do i need a writer pool for jdbc?  how is this drtributed?
          //val tableWriterPool = new TableWriterPool(jdbcMgr)
          while (recIter.hasNext) {
            val hbrowVal = recIter.next()
            val rec: HBRow = hbrowVal.asInstanceOf[HBRow]
            val rkey = rec.rowKey
            val recJson = Json(DefaultFormats).write(rec.cells)

            jdbcMgr.addToTableQueue(table, jdbcColumnValue.create("VARCHAR", recJson).get)
            tableCount += Tuple2(rec.tableName, 1)
            log.info(s"RDD_TRACE $batchTime writing into $dbName hbrow: $rkey")
          } //while hasNext

          // batch insert
          jdbcMgr.flushAll() match {
            case Failure(e) => {
              log.error(s"jdbc write failed! with ${e.toString}")
              totalWrite = 0
            }
            case Success(count) => totalWrite = count
          }
          // close hbase manager
          jdbcMgr.close()
        } //end of hasNext

        log.info(s"RDD_TRACE $batchTime: Exiting mapPartitions for jdbc write.")
        tableCount.iterator
      }) //end of mapPartitions
      totalWrite = tableCountSet.count()
      log.info(s"$appName Jdbc write wrote ${totalWrite.toString} Hbrows")
    }
    totalWrite
  }


}

