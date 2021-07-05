/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.streaming

import scala.collection.mutable.ArrayBuffer

import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, HdfsLogger, UnitOfWork}

import com.KanariDigital.app.{App, AppType, AppContext, RddProcessor, RddProcessorOpts}
import com.KanariDigital.app.util.{LogTag, SparkUtils}
import com.kanaridi.common.util.{HdfsUtils, LogUtils}

import com.KanariDigital.datasource.hdfs.rdd.HasFilePaths

import com.kanaridi.xform.JObjExplorer

import org.apache.commons.logging.{Log, LogFactory}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.KFileInputDStream
import org.apache.spark.storage.StorageLevel

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.immutable.{Map => ScalaMap}

import org.apache.hadoop.fs.{CreateFlag, FSDataOutputStream, GlobFilter, Options, Path, FileStatus, RemoteIterator, LocatedFileStatus, FileSystem}
import org.apache.spark.rdd.RDD
import java.util.Date

import org.apache.spark.sql.types._

import scala.util.parsing.json.JSONObject

/**
  */
class FlowApp(appName: String) extends
    App(appName, AppType.HDFS_STREAMING_APP_TYPE)
    with JObjExplorer{

  override val appTypeStr = AppType.FLOW_APP_TYPE

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

    // a factory function that can return a DataFrame writer for HBase
    def makeWriter(foName: String, flow: Any, connection: Any): (DataFrame) => Long = {

      val tableId = stringVal(flow, "options.topic", "none")

      /** persist dataframe to HBase via com.KanariDigital.Orderbook.RddProcessor
        *
      */
      def writeHBase(df: DataFrame) : Long = {
        try {

          val rows = df.rdd
          // an RDD of JSON representation of rows like df.toJSON, but eliminates nulls
          val jsonRDD = rows.map( row => {
            val fieldNames = row.schema.fieldNames
            val valMap = row.getValuesMap(fieldNames)
            val vm2 = valMap.filter( (y) =>  !Option(y._2).getOrElse("").isEmpty  ) // remove empty fields
            val jsonObj = JSONObject(vm2)
            val myMap: Map[String, JSONObject] = Map("data" -> jsonObj)
            val myJO = JSONObject(myMap)
            val result = myJO.toString()
            result
          })

          // make it a tuple with the mapping topic name
          val rddTopicAndJson = jsonRDD.map(x => (tableId, x)).persist

          val start = System.currentTimeMillis()

          // rddTopicAndJson.take(10).foreach(println)

          val rowsC = rddTopicAndJson.count()

          val unitOfWork =  com.KanariDigital.app.AuditLog.UnitOfWork.createUnitOfWork()
          val auditLog = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)
          val idocs = 0

          val opts: Set[RddProcessorOpts] = Set(RddProcessorOpts.REPART_SORT)
          val rddProcessor = new RddProcessor(appContext, appName, opts)
          val encounteredException = rddProcessor.processRDD(unitOfWork, start, rddTopicAndJson, auditLog)

          //
          if (encounteredException.isDefined) {
            appContext.hdfsAuditLogger.logException(encounteredException.get)
            //abort the job
            throw new Exception(encounteredException.get)
          }

          rddTopicAndJson.unpersist()

          val duration = System.currentTimeMillis() - start
          idocs

        } catch {
          case ex : Exception => {
            log.error(s"HDFS Folder processor, Exception: ${ex.toString()}")
            ex.printStackTrace
            return -1
          }
        }
      }

      writeHBase
    }

    log.info(s"$appName: runApp: start")
    val appConfig = appContext.appConfig

    // create spark context from spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession: SparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)

    val sparkContext = sparkSession.sparkContext

    // set log level
    val logLevel = appContext.appConfig.appConfigMap.getOrElse("log-level", "WARN")
    sparkContext.setLogLevel(logLevel)

    val checkpointDir = appContext.appConfig.appConfigMap.getOrElse("checkpoint", "/kanari/data/ob/temp")
    sparkContext.setCheckpointDir(checkpointDir)

    // runtime application id
    val runtimeApplicationId = sparkContext.applicationId
    appContext.appConfig.runtimeAppId = runtimeApplicationId

    // application id
    val applicationId = appConfig.getApplicationId

    // start processing files from this date time string
    // if no since exists in config file the start time is current time
    val startTimeStr: String = appConfig.hdfsStreamingConfigMap.getOrElse("since", "")
    var startTime = if (startTimeStr != "") SparkUtils.toMillis(startTimeStr) else System.currentTimeMillis

    log.warn(s"""$runtimeApplicationId - ${LogTag.START_APP} -
                 |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                 |message: starting application $applicationId $appName with $runtimeApplicationId""".stripMargin.replaceAll("\n", " "))


   //loop forever
    val mmcfg = appContext.flowSpec

    // inject a dataframe writer factory that uses message mapper to write to HBase
    mmcfg.connectionWriterFactories.put("hbase", makeWriter )

    val flowOpt = mmcfg.getDownstreamOp(appName, sparkSession)

    flowOpt match {

      case Some(flow) => flowOpt.get.run()
      case None => {

        log.warn(s"""$runtimeApplicationId - ${LogTag.START_APP} -
                 |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                 |message: no flow op for $applicationId $appName with $runtimeApplicationId""".stripMargin.replaceAll("\n", " "))
      }

    }
  }
}
