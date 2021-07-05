/* Copyright (c) 2020 Kanari Digital, Inc. */


package com.KanariDigital.app.streaming

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import com.KanariDigital.app.{App, AppContext, AppType, RddProcessor, RddProcessorOpts}
import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, UnitOfWork}
import com.KanariDigital.app.util.{LogTag, SparkUtils}
import com.kanaridi.common.util.HdfsUtils


/** Azure Blobstore File System (abfs) spark streaming app which periodically retrieves data
  * from files in abfs, transforms and writes the transformed data to a
  * series of hbase tables
  */
class AbfsStreamingApp(appName: String) extends
    HdfsStreamingApp(appName) {

  override val appTypeStr = AppType.ABFS_STREAMING_APP_TYPE


  /** run hdfs streaming application which processes raw data from hdfs
    * @param appName - application name
    * @param isLocal - set to true to create a local spark streaming context
    * @param appContext - application context which contains,
    *                     [[MessageMapperFactory]]  and [[AppConfig]] objects
    */
  override def runApp(
    appContext: AppContext,
    isLocal: Boolean) = {

    log.info(s"$appName: runApp: start")

    // create spark context from spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession: SparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)

    val sparkContext = sparkSession.sparkContext
    // set log level
    val logLevel = appContext.appConfig.appConfigMap.getOrElse("log-level", "WARN")
    sparkContext.setLogLevel(logLevel)

    val sqlContext = sparkSession.sqlContext

    // runtime application id
    val runtimeApplicationId = sparkContext.applicationId
    appContext.appConfig.runtimeAppId = runtimeApplicationId

    val appConfig = appContext.appConfig

    // application id
    val applicationId = appConfig.getApplicationId

    // abfs storage account abfs://financepoc@ssap.dfs.core.windows.net
    val abfsUrl  = appConfig.abfsStreamingConfigMap.getOrElse("abfs-url", "")
    val abfsDir = if (abfsUrl !="") abfsUrl else {
      val storageAccount = appConfig.abfsStreamingConfigMap("storage-account")
      val container = appConfig.abfsStreamingConfigMap("container")
      s"""abfs://$container@$storageAccount"""
    }

    // abfs streaming app
    val incoming = appConfig.abfsStreamingConfigMap("incoming")
    val incomingDirs = incoming.split(",").map(x => new Path(abfsDir, x)).mkString(",")

    // abfs scratch dir
    val scratchDir = appConfig.abfsStreamingConfigMap.getOrElse("scratch-dir", "")

    // mergee files
    val mergeSourceFiles = appConfig.abfsStreamingConfigMap.getOrElse("merge-source-files", "false").toBoolean

    // move files to archive
    val archiveDir = appConfig.abfsStreamingConfigMap.getOrElse("archive", "")

    val maxFilesBatch:Int = appConfig.abfsStreamingConfigMap.getOrElse("files-batch-size", "1").toInt

    // process files that have last modified timestamp than slack time
    val modifiedAgoSeconds: Long = appConfig.abfsStreamingConfigMap.getOrElse("files-modified-ago", "600").toLong
    val modifiedAgo = modifiedAgoSeconds * 1000

    // bstl expression to select sort field in incoming records
    //$.header.timestamp
    val sortDataBy: String = appConfig.abfsStreamingConfigMap.getOrElse("files-sort-data-by", "")

    // max partitions allowed
    val maxFilesPartitions: Int = appConfig.abfsStreamingConfigMap.getOrElse("files-max-partitions", "180").toInt

    // topic
    val abfsTopics = appConfig.abfsStreamingConfigMap("topics")
    val abfsTopicJsonPath = appConfig.abfsStreamingConfigMap.getOrElse("topic-json-path", "")

    // format and schema of the files to ingest
    val filesFormat = appConfig.abfsStreamingConfigMap.getOrElse("files-format", "json")

    // get reading options for format type
    val filesOptions: Map[String, String] = if (filesFormat == "csv") {
      Map(
        "header" -> appConfig.abfsStreamingConfigMap.getOrElse("files-format-csv-header", "true"),
        "sep" -> appConfig.abfsStreamingConfigMap.getOrElse("files-format-csv-separator", ","),
        "inferSchema" -> appConfig.abfsStreamingConfigMap.getOrElse("files-format-csv-inferschema", "false")
      )
    } else if (filesFormat == "parquet") {
      Map(
        "mergeSchema" -> appConfig.abfsStreamingConfigMap.getOrElse("files-format-parquet-mergeschema", "true")
      )
    } else {
      //default options
      Map.empty[String, String]
    }

    // schema
    val filesSourceSchema: String = if (filesFormat == "csv") {
      appConfig.abfsStreamingConfigMap.getOrElse("files-format-csv-schema", "")
    } else {
      ""
    }

    // duration
    val durationSeconds = appConfig.sparkConfigMap("batchDuration").toLong
    val duration = durationSeconds * 1000

    // start processing files from this date time string
    // if no since exists in config file the start time is current time
    val startTimeStr: String = appConfig.abfsStreamingConfigMap.getOrElse("since", "")

    // source file pattern
    val filenameRegex = appConfig.abfsStreamingConfigMap.getOrElse("filename-regex", ".+")
    val directoryRegex = appConfig.abfsStreamingConfigMap.getOrElse("directory-regex", ".+")

    log.warn(s"""$runtimeApplicationId - ${LogTag.START_APP} -
                 |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                 |message: starting application $applicationId $appName with $runtimeApplicationId""".stripMargin.replaceAll("\n", " "))

    iterateDirs(appContext, sparkContext, incomingDirs, scratchDir, mergeSourceFiles,
      archiveDir, maxFilesBatch, modifiedAgo,
      sortDataBy, maxFilesPartitions: Int, abfsTopicJsonPath, filesFormat,
      filesOptions, filesSourceSchema, duration, startTimeStr,
      filenameRegex, directoryRegex)
  }
}
