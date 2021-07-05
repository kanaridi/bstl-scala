/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.util

import scala.util.{Try, Success, Failure}
import com.fasterxml.jackson.databind.ObjectMapper

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

import com.kanaridi.bstl.JsonPath
import com.microsoft.azure.eventhubs.EventData

import java.nio.charset.Charset

import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;

import com.KanariDigital.app.AppContext
import com.kanaridi.xform.HBRow
import com.kanaridi.common.util.LogUtils

/** utils for spark */
object SparkUtils {

  val log: Log = LogFactory.getLog(SparkUtils.getClass.getName)

  /** get a SparkSession object which contains SparkContext and SQLContext
    *
    * @param appName application to run
    * @param isLocal set to 'true' to create local spark context. 'false' to create a remote spark context.
    *
    * @return SparkSession object
    */
  def createSparkSession(
    appName: String,
    sparkConf: SparkConf,
    isLocal: Boolean): SparkSession = {

    log.info(s"$appName: Creating Spark Session...")

    val builder: Builder =  new Builder().config(sparkConf)

    if (isLocal) {
      log.info(s"$appName: Running LOCAL spark instance")
      builder.master("local[*]")
    } else {
      log.info(s"$appName: Spark running as Yarn [client | cluster]")
    }

    builder.getOrCreate()
  }

  /** get {{StorageLevel}} for specified string
    * @param storageLevelStr - storage level string e.g. "MEMORY_AND_DISK", "MEMORY_AND_DISK_SER2"
    *
    * @return {{org.apache.spark.storage.StorageLevel}}
    */
  def getStorageLevel(storageLevelStr: String): StorageLevel = {
    storageLevelStr match {
      case "MEMORY_AND_DISK_2" => StorageLevel.MEMORY_AND_DISK_2
      case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
      case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_ONLY_2" => StorageLevel.MEMORY_ONLY_2
      case "MEMORY_ONLY_SER_2" => StorageLevel.MEMORY_ONLY_SER_2
      case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
      case "NONE" => StorageLevel.NONE
      case "OFF_HEAP" => StorageLevel.OFF_HEAP
      //default
      case "" => StorageLevel.MEMORY_AND_DISK_2
    }
  }

  /** sort data by specified field in incoming data
    * @param sourceRDD - source RDD to sort
    * @param sortDataBy - jsonpath expression that selects timestamp field
    *
    * @return RDD with elements sorted by timestamp in ascending order
    */
  def sortData(sourceRDD: RDD[String], sortDataBy: String): RDD[String] = {

    val keyValRDD = sourceRDD.map(fileRDD => {

      val jobj = (new ObjectMapper).readValue(fileRDD.toString, classOf[Object])
      val qr = JsonPath.query(sortDataBy, jobj)
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
      new Tuple2(timestampIntVal, fileRDD.toString)
    })

    //sort by the key
    val sortedFilesData = keyValRDD.sortByKey()

    sortedFilesData.map(rdd => {
      rdd._2.toString
    })
  }

  /** sort data by specified field in incoming data
    * @param sourceRDD - source RDD to sort
    * @param sortDataBy - jsonpath expression that selects timestamp field
    * @param applicationId - application id for logging purposes
    * @param batchTime - batch time for logging purposes
    *
    * @return RDD with elements sorted by timestamp in ascending order
    */
  def sortDataDF(topicContentRDD: RDD[(String, String)], sortDataBy: String, batchTime: Long, appContext: AppContext): RDD[(String, String)] = {

    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)
    val applicationId = appContext.appConfig.appId
    val runtimeApplicationId = appContext.appConfig.runtimeAppId
    val appName = appContext.appConfig.appName
    val appTypeStr = appContext.appConfig.appType.toString

    val keyValRDD: RDD[(Long, String, String)] = topicContentRDD.mapPartitions(rddIter => {

      var keyValuePairList = scala.collection.mutable.ArrayBuffer.empty[Tuple3[Long, String, String]]

      while (rddIter.hasNext) {

        val rddElem = rddIter.next

        val (topic, content) = rddElem

        try {

          val jobj = (new ObjectMapper).readValue(content.toString, classOf[Object])
          val qr = JsonPath.query(sortDataBy, jobj)
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
          val keyValPair = new Tuple3(timestampIntVal, topic, content)
          keyValuePairList += keyValPair

        } catch {

          case ex: Exception => {

            val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)
            val contentStr = LogUtils.convToJsonString(content)

            log.error(s"""$runtimeApplicationId - ${LogTag.TRANSFORM_DATA_FAILED} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
              |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
              |sourceRecord: $contentStr,
              |action: discarded,
              |message: error extracting timestamp for sorting $exMesg"""
              .stripMargin.replaceAll("\n", " "))

          }
        }
      }
      keyValuePairList.iterator
    })

    // val keyValRDD = sourceRDD.map(fileRDD => {

    //   val jobj = (new ObjectMapper).readValue(fileRDD.toString, classOf[Object])
    //   val qr = JsonPath.query(sortDataBy, jobj)
    //   val timestampVal = qr match {
    //     case Left(x) => "0"
    //     case Right(x) => {
    //       val item0 = qr.right.map(_.toVector).right.get
    //       if (item0.length > 0 ) item0(0)
    //     }
    //   }
    //   val timestampIntValMaybe = Option(timestampVal.toString.toLong)
    //   val timestampIntVal:Long = timestampIntValMaybe match {
    //     case Some(i) => i
    //     case None => 0
    //   }
    //   //extract the timestamp
    //   new Tuple2(timestampIntVal, fileRDD.toString)
    // })

    //sort dataframe by timestamp
    val sparkSession = SparkSession.builder().getOrCreate()
    val keyValDF = sparkSession.createDataFrame(keyValRDD).toDF("timestamp", "topic", "content")
    val sortedKeyValDF = keyValDF.orderBy(asc("timestamp"))
    val sortedContentRDD: RDD[Row] = sortedKeyValDF.rdd
    val sortedContent = sortedContentRDD.map( row => {
      //get the topic, content fields
      (row.getString(1), row.getString(2))
    })
    sortedContent
  }

  /** get list of distinct topics in the topic-content RDD
    * @param sourceRDD - source RDD to sort
    *
    * @return list with distinct topics
    */
  def getDistinctTopicsDF(topicContentRDD: RDD[(String, String)]): List[String] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val keyValDF = sparkSession.createDataFrame(topicContentRDD).toDF("topic", "content")
    val topicsRDD: RDD[String] = keyValDF.select(keyValDF("topic")).distinct().rdd.map(row => { row.getString(0) })
    val topics = topicsRDD.collect()
    topics.toList
  }

  /** get list of distinct table names in the hbrow rdd
    *
    * @return list with distinct tablenames
    */
  def getDistinctTables(hbrowRDD: RDD[HBRow]): List[String] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val hbrowDF = sparkSession.createDataFrame(hbrowRDD).toDF
    val tableNameRDD: RDD[String] = hbrowDF.select(hbrowDF("tableName")).distinct().rdd.map(row => { row.getString(0) })
    val tableNames = tableNameRDD.collect()
    tableNames.toList
  }

  /** get a RDD where all elements are annotated by topic string
    * @param topic - topic json path string
    * @param sourceRDD - source rdd
    * @param applicationId - application id for logging purposes
    * @param batchTime - batch time for logging purposes
    *
    * @return  RDD[(topic, content)] rdd
    */
  def getTopicContentRDD(topicJsonPath: String, sourceRDD: RDD[String],
    batchTime: Long, appContext: AppContext): RDD[(String, String)] = {

    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)
    val runtimeApplicationId = appContext.appConfig.runtimeAppId
    val applicationId = appContext.appConfig.appId
    val appName = appContext.appConfig.appName
    val appTypeStr = appContext.appConfig.appType.toString

    val topicContentRDD: RDD[(String, String)] = sourceRDD.mapPartitions(rddIter => {

      var topicContentPairList = scala.collection.mutable.ArrayBuffer.empty[Tuple2[String, String]]

      while (rddIter.hasNext) {

        val sourceRDDElem = rddIter.next

        val sourceRDDStr = sourceRDDElem.toString

        try {

          if (topicJsonPath.contains("$") ) {
            //extract topic
            val jobj = (new ObjectMapper).readValue(sourceRDDStr, classOf[Object])
            val qr = JsonPath.query(topicJsonPath, jobj)
            val topic = qr match {
              case Left(x) => "unknown"
              case Right(x) => {
                val item0 = qr.right.map(_.toVector).right.get
                if (item0.length > 0 ) item0(0)
              }
            }

            val topicStrMaybe = Option(topic.toString)
            val topicStrVal:String = topicStrMaybe match {
              case Some(i) => i
              case None => "unknown"
            }

            var keyValPair = Tuple2(topicStrVal, sourceRDDStr)
            topicContentPairList += keyValPair

          } else {
            //topic is a literal string
            val topicStrVal = topicJsonPath
            var keyValPair = Tuple2(topicStrVal, sourceRDDStr)
            topicContentPairList += keyValPair
          }

        } catch {

          case ex: Exception => {

            val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)
            val contentStr = LogUtils.convToJsonString(sourceRDDStr)

            log.error(s"""$runtimeApplicationId - ${LogTag.TRANSFORM_DATA_FAILED} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
              |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
              |sourceRecord: $contentStr,
              |action: discarded,
              |message: error extracting topic info $exMesg"""
              .stripMargin.replaceAll("\n", " "))

          }
        }

      }
      topicContentPairList.iterator
    })
    topicContentRDD
  }

  /** get a RDD where all elements are annotated by topic string
    * @param topic - topic string
    * @param sourceRDD - source rdd
    *
    * @return  RDD[(topic, content)] rdd
    */
  def getEventhubTopicContentRDD(topic: String, sourceRDD: RDD[EventData]): RDD[(String, String)] = {

    val topicContentRDD: RDD[(String, String)] = sourceRDD.mapPartitions(rddIter => {

      var topicContentPairList = scala.collection.mutable.ArrayBuffer.empty[Tuple2[String, String]]

      while (rddIter.hasNext) {

        val sourceRDDIter = rddIter.next
        val sourceRDDBytes = Option(sourceRDDIter.getBytes)

        sourceRDDBytes match {
          case Some(elem) => {
            var keyValPair = new Tuple2(topic, new String(elem,
              Charset.forName("UTF-8")))
            topicContentPairList += keyValPair
          }
          case None => {
            val sourceRDDObject = Option(sourceRDDIter.getObject)
          }
        }
      }
      topicContentPairList.iterator
    })

    topicContentRDD
  }

  /** get applicationId for spark application  */
  def getRuntimeApplicationId(): String = {
    Try {
      val sparkSession = SparkSession.builder().getOrCreate()
      val sc = sparkSession.sparkContext
      sc.applicationId
    } match {
      case Success(x) => x
      case Failure(ex) => {
        val mesg = ex.getStackTrace().map(_.toString).mkString(" ")
        log.error(s"get application id error - errorMessage $mesg")
        ""
      }
    }
  }

  /** get applicationId for spark application  */
  def getSparkConfProperty(key: String, defaultVal: String): String = {
    Try {
      val sparkSession = SparkSession.builder().getOrCreate()
      val sc = sparkSession.sparkContext
      val sconf: SparkConf = sc.getConf
      val propVal = sconf.get(key, defaultVal).toString
      propVal
    } match {
      case Success(x) => x
      case Failure(ex) => {
        val mesg = ex.getStackTrace().map(_.toString).mkString(" ")
        log.error(s"get spark conf property error - errorMessage $mesg")
        ""
      }
    }
  }

  /** format milliseconds to a string representation of datetime
    * @param milliseconds - milliseconds to convert
    * @return formatted date string yyyy-MM-dd'T'hh:mm:ss format
    */
  def formatBatchTime(milliseconds: Long): String = {
    Try {
      val batchDateTime = new Date(milliseconds)
      val currentBatchTimeStr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(batchDateTime)
      currentBatchTimeStr
    } match {
      case Success(x) => x
      case Failure(ex) => {
        val mesg = ex.getStackTrace().map(_.toString).mkString(" ")
        log.error(s"format batch time - errorMessage $mesg")
        ""
      }
    }
  }

  /** format milliseconds to a string representation of datetime
    * @param formatted date string yyyy-MM-dd'T'hh:mm:ss iso format
    * @return milliseconds - milliseconds
    */
  def toMillis(formattedStr: String): Long = {
    val dateMayBe = Option(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(formattedStr))
    dateMayBe match {
      case Some(date) => date.getTime
      case None => new Date().getTime //get now
    }
  }

  /** set logLevel for spark
    *
    */
  def setSparkDefaultLogLevel(logLevel: String): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel(logLevel)
  }

}
