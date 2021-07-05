/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.streaming

import scala.collection.mutable.ArrayBuffer

import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, HdfsLogger, UnitOfWork}


import com.KanariDigital.app.{App, AppType, AppContext, RddProcessor}
import com.KanariDigital.app.util.SparkUtils
import com.kanaridi.common.util.HdfsUtils
import com.KanariDigital.datasource.hdfs.rdd.HasFilePaths

import com.kanaridi.bstl.JsonPath

import org.apache.commons.logging.{Log, LogFactory}

import org.apache.hadoop.fs.{GlobFilter, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.KFileInputDStream

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.immutable.{Map => ScalaMap}


/** HDFS spark streaming app which periodically retrieves data
  * from files in hdfs, transforms and writes the transformed data to a
  * series of hbase tables
  */
class HdfsFileDStreamingApp(appName: String) extends
    App(appName, AppType.HDFS_DSTREAM_APP_TYPE) {

  val log: Log = LogFactory.getLog(this.getClass.getName)

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

    val appConfig = appContext.appConfig

    val duration = appConfig.sparkConfigMap("batchDuration").toLong
    log.info(s"$appName: runApp: Creating Spark Streaming Context with batch duration of $duration seconds")

    // get a spark session
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)

    // hdfs streaming app
    val incomingDir = appConfig.hdfsStreamingConfigMap("incoming")
    val archiveDir = appConfig.hdfsStreamingConfigMap("archive")

    // topic
    val hdfsTopic = appConfig.hdfsStreamingConfigMap("topic")

    // ignore any files after specified time
    val rememberDuration = appConfig.hdfsStreamingConfigMap("files-remember-duration")
    sparkConf.set(
      "spark.streaming.minRememberDuration",
      rememberDuration)

    // source file pattern
    val incomingFilePatternStr = appConfig.hdfsStreamingConfigMap("file-pattern")
    val incomingPathFilter = (path: Path) => {
      val globFilter = new GlobFilter(incomingFilePatternStr)
      globFilter.accept(path)
    }

    // source file pattern
    val newFilesOnly: Boolean = appConfig.hdfsStreamingConfigMap("new-files-only").toBoolean

    val sparkSession: SparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val ssc = new StreamingContext(sparkContext, Seconds(duration))

    //create a dstream
    val stream = new KFileInputDStream[LongWritable, Text, TextInputFormat](
      ssc,
      incomingDir,
      incomingPathFilter,
      newFilesOnly)

    // extract batch time string
    var currentBatchMillis = 0L
    var files:ArrayBuffer[String] = ArrayBuffer.empty[String]
    var uow : UnitOfWork = null

    val messagesWithBatchTime = stream.transform { (rdd, batchTime) =>

      //set new batch time
      currentBatchMillis = batchTime.milliseconds
      val logBatchTime = SparkUtils.formatBatchTime(currentBatchMillis)
      val filePaths = if (rdd.isInstanceOf[HasFilePaths]) rdd.asInstanceOf[HasFilePaths].filePaths else Seq()

      filePaths.foreach(filePath => {
        log.info(s"$appName: batch: {$logBatchTime}: adding file: {${filePath} ...")
        files += filePath
      })

      //set unit of work
      uow = UnitOfWork.createUnitOfWork()

      rdd
    }

    val timestampMessage = messagesWithBatchTime.map(unionRDD => {
      log.info("unionRDD: " + unionRDD.toString)
      val jobj = (new ObjectMapper).readValue(unionRDD._2.toString, classOf[Object])
      val qr = JsonPath.query("$.header.timestamp", jobj)
      val timestampVal = qr match {
        case Left(x) => "0"
        case Right(x) => {
          val item0 = qr.right.map(_.toVector).right.get
          if (item0.length > 0 ) item0(0)
        }
      }
      val timestampIntValMaybe = Option(timestampVal.toString.toLong)
      val timestampIntVal:Long = timestampIntValMaybe match {
        case Some(i) => i
        case None => 0
      }
      //extract the timestamp
      (timestampIntVal, unionRDD._2.toString)

    })

    //sort dstream by timestamp
    val sortedMessages = timestampMessage.transform(rdd => rdd.sortBy(_._1))

    val topicAndJson = sortedMessages.map(rdd => {
      (hdfsTopic, rdd._2.toString)
    })

    var encounteredException : Option[Exception] = None

    topicAndJson.foreachRDD( rdd => {

      rdd.persist()

      // check of consumer record rdd has any elements
      if (rdd.isEmpty()) {
        log.info(s"$appName:  Nothing to process ...")
      } else {
        log.info(s"$appName:  Found data to process ...")

        val auditLog = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)

        // process and transform data
        val rddProcessor = new RddProcessor(appContext, appName)
        encounteredException = rddProcessor.processRDD(uow, rdd, auditLog)

        // if (encounteredException.isDefined == false) {
        //  //wite the raw records to hdfs
        //  encounteredException = storeRawData(appContext, appName, rdd)
        // }

        if (encounteredException.isDefined) {
          appContext.hdfsAuditLogger.logException(encounteredException.get)
        } else {
          //since processing was sucessful move files in this batch to archive
          val logBatchTimeStr = SparkUtils.formatBatchTime(currentBatchMillis)

          if (incomingDir.endsWith("*/")) {
            HdfsUtils.moveFilesPreservePath(files.toArray, archiveDir)
          } else {
            HdfsUtils.moveFiles(files.toArray, archiveDir + logBatchTimeStr.replaceAll(":", "_"))
          }
          log.info(s"$appName: About to log files that were processed to the audit logs: new: count={" + files.size.toString() + "}...")

          log.info(s"$appName: log files processed to the audit logs done.")
          //going to log the offsets
          val auditLogger = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)
          files.foreach(oneFile => {
            val logJson = fileToJSONString(appName, currentBatchMillis, oneFile)
            auditLogger.insertLogMessage(s"$uow: " + logJson)
          })
          auditLogger.flush()

          //reset offsetArray
          files = ArrayBuffer.empty[String]

          log.info(s"$appName: log files processed to the audit logs done.")
        }
      }
      rdd.unpersist()
    })

    // start streaming job
    ssc.start()
    ssc.awaitTermination()
  }

  /** create a json string of files that were processed for the audit logs
    */
  private def fileToJSONString(appName: String, batchTime: Long, file: String): String = {

      val offsetMap = Map(
        "appName" -> appName,
        "batchTime" -> batchTime,
        "batchTimeStr" -> SparkUtils.formatBatchTime(batchTime),
        "files" -> file)

      //parse map as json
      val jsonCellsMapper = new ObjectMapper()
      jsonCellsMapper.registerModule(DefaultScalaModule)
      val offsetRangeJson = jsonCellsMapper.writeValueAsString(offsetMap)
      offsetRangeJson
  }
}
