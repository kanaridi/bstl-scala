/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.util

import scala.collection.mutable.{ArrayBuffer => MutableArrayBuffer}
import scala.util.{Failure, Success, Try}

import com.KanariDigital.TableWriterPool
import com.KanariDigital.app.AppContext
import com.KanariDigital.hbase.HBaseManager

import com.kanaridi.xform.HBRow

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.rdd.RDD
import com.kanaridi.common.util.LogUtils

/** util to write contents of hbrows rdd to hbase
  */
object HBaseWriteUtils extends Serializable {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** write contents of {{ RDD[HBRow] }} to hbase
    *
    * @return number of records written to hbase
    */
  def writeResultsToHBase(
    appContext: AppContext,
    batchTime: Long,
    hbrows: RDD[HBRow]): Long = {

    val appConfig = appContext.appConfig

    // hbase connection properties
    val hbaseConfigMap = appConfig.hbaseConfigMap

    // enabled by default
    val hbaseWriteEnabled = hbaseConfigMap.getOrElse("enabled", "true")

    val zookeeperQuorum = hbaseConfigMap.getOrElse("zookeeper.quorum", "")
    val zookeeperClientPort = hbaseConfigMap.getOrElse("zookeeper.client.port", "")
    val kerberosPrincipal = hbaseConfigMap.getOrElse("kerberos.principal", "")
    val kerberosKeytab = hbaseConfigMap.getOrElse("kerberos.keytab", "")
    val distributedMode = hbaseConfigMap.getOrElse("distributed.mode", "false")
    val scannerCaching = hbaseConfigMap.getOrElse("scannerCaching", "")

    // app properties
    val appConfigMap = appConfig.appConfigMap
    val enableDedup = appConfigMap.getOrElse("enable-dedup", "false")
    val dedupColFamily = appConfigMap.getOrElse("dedup-colfamily", "d")
    val dedupColName = appConfigMap.getOrElse("dedup-colname", "dedup_hash")
    val dedupLookback = appConfigMap.getOrElse("dedup-lookback", "0")
    val maxBatchSize = appConfigMap.getOrElse("max-batch-size", "1000")

    //log.info(s"RDD_TRACE $batchTime Finished XForm, Starting HBase")

    // app
    val applicationId = appConfig.getApplicationId
    val appName = appConfig.appName
    val appTypeStr = appConfig.appType
    val runtimeApplicationId = appConfig.runtimeAppId
    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)

    // try
    Try {

      // write to eventhub if enabled
      if (hbaseWriteEnabled == "true") {

        val start = System.currentTimeMillis

        val tableCountSet: RDD[(String, Int)] = hbrows.mapPartitionsWithIndex((partIndex, recIter) => {

          log.info(s"RDD_TRACE $batchTime: Entering mapPartitions...")

          // log.warn(s"SENDING partIndex: {$partIndex} start")

          var tableCount = MutableArrayBuffer.empty[Tuple2[String, Int]]

          if (recIter.hasNext) {

            // keep track of row-key's
            var rowkeyLookupMap = scala.collection.mutable.HashMap.empty[String, String]

            val hbaseManager =
              HBaseManager.create(
                zookeeperQuorum, zookeeperClientPort,
                kerberosPrincipal, kerberosKeytab,
                distributedMode, scannerCaching)

            val tableWriterPool = new TableWriterPool(hbaseManager)

            // log.info(s"RDD_TRACE $batchTime Iterate over all rdd elements in a partition")

            while (recIter.hasNext) {

              val hbrowVal = recIter.next()

              val rec: HBRow = hbrowVal.asInstanceOf[HBRow]

              // log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_HBASE} -
              //   |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              //   |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
              //   |message: SENDING DATA ${rec.tableName} ${rec.sortKey} """
              //   .stripMargin.replaceAll("\n", " "))

              tableCount += Tuple2(rec.tableName, 1)

              val rkey = rec.rowKey
              val tname = rec.tableName
              val dedupKey = rec.deDupKey

              log.info(s"RDD_TRACE $batchTime writing into $tname hbrow: $rkey")

              //prepare table writer
              val tableWriter = tableWriterPool.getOrCreate(rec.namespace, rec.tableName)
              tableWriter.enableDedup = enableDedup
              tableWriter.dedupColFamily = dedupColFamily
              tableWriter.dedupColName = dedupColName
              tableWriter.dedupLookback = dedupLookback

              // check if we have seen the table row key before, in row lookup map
              val lookupTableRowKey = tname + "_" + rkey

              var currentBatchSize = tableWriter.getCurrentBatchSize()

              // write the current batch to hbase if we see a row key
              // we have seen before OR if the current batch size is greater than max batch size.
              if ( (rowkeyLookupMap contains lookupTableRowKey)
                || currentBatchSize > maxBatchSize.toInt) {

                // log.info(s"RDD_TRACE $batchTime rowkey: {$lookupTableRowKey} already EXISTS, flush table {$tname}")

                // write to hbase
                tableWriter.flush()

                // clear out all the keys from rowkeyLookupMap, only for table
                val removeKeys = rowkeyLookupMap.keySet
                  .filter(keyStr => keyStr.startsWith(tname))
                removeKeys.foreach(key => {rowkeyLookupMap.remove(key)})

                // start a new batch
                // log.info(s"RDD_TRACE $batchTime: starting  a NEW BATCH ...")
                tableWriter.insertLines(rkey, rec.family, rec.cells, dedupKey)

                //add the new row ke
                rowkeyLookupMap.put(lookupTableRowKey, lookupTableRowKey)

              } else {

                // log.info(s"RDD_TRACE $batchTime rowkey {" + lookupTableRowKey + "} is NEW, add to existing batch")

                // add rows to a batch
                tableWriter.insertLines(rkey, rec.family, rec.cells, dedupKey)

                //add the row key for tracking
                rowkeyLookupMap.put(lookupTableRowKey, lookupTableRowKey)

              }
              //})

            } //while hasNext

            // batch insert
            tableWriterPool.flush()

            //clear rowkeyLookupMap
            rowkeyLookupMap.clear()

            // close hbase writer's
            tableWriterPool.close()

            // close hbase manager
            hbaseManager.close()

          } //end of hasNext

          log.info(s"RDD_TRACE $batchTime: Exiting mapPartitions.")

          //topicDistinctIDocIds.iterator

          // log.warn(s"SENDING partIndex: {$partIndex} finish numItemsInPartition ${tableCount.size}")

          tableCount.iterator

        }) //end of mapPartitions

        // cache rdd
        tableCountSet.persist()

        // count records by tableName
        val tableTotalCountRDD:RDD[(String, Int)] = tableCountSet.reduceByKey(_ + _)

        // call an action
        tableTotalCountRDD.foreach(tableCount => {
          val (tableName, count) = tableCount
          if (count > 0) {
            val tableTotalDuration = System.currentTimeMillis - start
            log.warn(s"""$runtimeApplicationId - ${LogTag.HBASE_PUT_TABLE_COUNT} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
              |table: $tableName, count: $count,
              |duration: $tableTotalDuration,
              |message: wrote $count records to hbase table $tableName in $tableTotalDuration milliseconds"""
              .stripMargin.replaceAll("\n", " "))
          }
        })

        // total hbrows inserted to hbase
        val totalCountHBase = tableTotalCountRDD.values.sum().toLong

        // remove cached rdd
        tableCountSet.unpersist()

        val duration = System.currentTimeMillis - start

        log.info(s"RDD_TRACE $batchTime Finished XForm, Finished HBase")

        val totalDuration = System.currentTimeMillis - start
        log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_HBASE} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |count: $totalCountHBase,
          |duration: $totalDuration,
          |message: write $totalCountHBase records to hbase in $totalDuration milliseconds"""
          .stripMargin.replaceAll("\n", " "))

        // num of records
        totalCountHBase

      } else {

        log.info(s"$appName skip writing data to hbase enabled flag is set to $hbaseWriteEnabled...")
        // return
        0
      }

    } match {
      case Success(x) => {
        // success return number of rows inserted
        val totalCount = x.asInstanceOf[Long]
        totalCount
      }
      case Failure(ex) => {
        // ex.printStackTrace()
        // failed
        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""$runtimeApplicationId - ${LogTag.APP_ERROR_EVENTHUB_WRITE_FAILED} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
          |message: error storing data to eventhub $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        throw ex
      }
    }
  }
}
