/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.util

import java.text.SimpleDateFormat
import java.time.Instant

import scala.collection.immutable.{Map => ImmutableMap}

import com.KanariDigital.app.AppContext

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SQLContext, SaveMode, SparkSession}
import com.kanaridi.xform.HBRow
import com.kanaridi.common.util.LogUtils


/** utils for storing hbrow data in hdfs
  */
object HdfsWriteUtils extends Serializable {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** store contents of hbrow RDD in HDFS
    *
    * {{
    *   $STORE_RAW_PATH/yearmonth={$YYYY}{$MM}/daytimestamp={$dd}{$HH}{$mm}{$ss}/part-*.txt
    * }}
    *
    *  e.g.
    *
    * {{
    *  /HDFS_ROOT/EA/supplychain/raw/ob/ORDERS/yearmonth=201907/daytimestamp=17205959/part-00000-63b71f8a-be42-4a3f-8358-0715a8addea3.txt
    * }}
    */
  def writeResultsToHdfs(
    appContext: AppContext,
    batchTime: Long,
    hbrows: RDD[HBRow]): Unit = {

    val appConfig = appContext.appConfig

    val hdfsWriteConfigMap = appConfig.hdfsWriteConfigMap

    val hdfsWriteMappingConfigMap = appConfig.hdfsWriteMappingConfigMap

    // not enabled by default

    val storeEnabled = hdfsWriteConfigMap.getOrElse("enabled", "false")
    val storePath = hdfsWriteConfigMap.getOrElse("path", "")
    val storeFormat = hdfsWriteConfigMap.getOrElse("format", "")
    val storeMapping: ImmutableMap[String, String] = hdfsWriteMappingConfigMap

    log.warn(s"hdfsWriteEnabled: $storeEnabled")

    val defaultStorageLevelStr = appConfig.appConfigMap.getOrElse(
      "default-storage-level",
      "MEMORY_AND_DISK_SER")

    //app
    val applicationId = appConfig.getApplicationId
    val appName = appConfig.appName
    val appTypeStr = appConfig.appType
    val runtimeApplicationId = appConfig.runtimeAppId
    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)

    //get existing spark session and sqlcontext
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    val sqlContext = sparkSession.sqlContext

    try {

      if (storeEnabled == "true") {

        log.info(s"$appName hdfsWriteEnabled is true: Writing Raw JSON to HDFS...")

        //get distinct tablenames in hbrow
        val tableNames = SparkUtils.getDistinctTables(hbrows)


        tableNames.foreach(tableName => {
          log.warn(s"$appName Writing tableName $tableName to hdfs...")

          StoreRawUtils.writeRecords(
            appName, sqlContext,
            storeFormat, storePath,
            storeMapping, tableName,
            hbrows.filter(pr => pr.tableName == tableName)
          )

          log.warn(s"$appName Writing tableName $tableName to hdfs done.")

        })
      } else {
        log.warn(s"$appName hdfsWriteEnabled is false: Skipping Raw JSON writing.")
      }

    } catch {

      case ex: Exception => {

        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""$runtimeApplicationId - ${LogTag.APP_ERROR} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
          |message: error storing hbrow data to hdfs $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        throw ex
      }
    }

  }

}
