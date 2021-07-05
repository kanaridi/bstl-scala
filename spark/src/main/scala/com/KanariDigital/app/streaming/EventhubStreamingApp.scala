/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.streaming

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.{Map => ImmutableMap}
import scala.collection.mutable.{Map => MutableMap}

import scala.util.{Try, Success, Failure}

import com.KanariDigital.app.{App, AppType, AppContext, RddProcessor, RddProcessorOpts}
import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, HdfsLogger, UnitOfWork}
import com.KanariDigital.app.util.{SparkUtils, StoreRawUtils, LogTag, EventhubWriteUtils}
import com.kanaridi.common.util.{LogUtils, RetryUtils}

import com.KanariDigital.datasource.eventhub.{EventHubOffsetSpec, EventHubOffsetSpecs}

import com.kanaridi.bstl.JsonPath

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition,EventHubsUtils}
import org.apache.spark.eventhubs.rdd.{HasOffsetRanges => EventHubHasOffsetRanges, OffsetRange => EventHubOffsetRange}

import java.time.Duration
import java.nio.charset.Charset


/** Eventhub spark streaming app which retrieves data from eventhub,
  * transforms the data and writes the data into one or more hbase tables
  * or eventhub
  */
class EventhubStreamingApp(appName: String) extends
    App(appName, AppType.EVENTHUB_STREAMING_APP_TYPE) {

  override val appTypeStr = AppType.EVENTHUB_STREAMING_APP_TYPE

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** run eventhub application which processes raw data from eventhub
    * @param appName - application name
    * @param isLocal - set to true to create a local spark streaming context
    * @param appContext - application context which contains,
    *                     [[MessageMapperFactory]]  and [[AppConfig]] objects
    */
  override def runApp(appContext: AppContext, isLocal: Boolean) = {

    log.info(s"$appName: runApp: start")

    // create spark context from spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession: SparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)

    val sparkContext = sparkSession.sparkContext

    val sqlContext = sparkSession.sqlContext

    // runtime application id
    val runtimeApplicationId = sparkContext.applicationId
    appContext.appConfig.runtimeAppId = runtimeApplicationId

    val appConfig = appContext.appConfig

    // set log level
    val logLevel = appConfig.appConfigMap.getOrElse("log-level", "WARN")
    sparkContext.setLogLevel(logLevel)

    val duration = appConfig.sparkConfigMap("batchDuration").toLong
    log.info(s"$appName: runApp: Creating Spark Streaming Context with batch duration of $duration seconds")

    // app id
    val applicationId = appConfig.getApplicationId

    // get eventhub configuration
    val eventhubEndpoint = appConfig.eventhubConfigMap.getOrElse("endpoint", "")

    // eventhub connection parts
    val eventhubNamespace = appConfig.eventhubConfigMap.getOrElse("namespace", "")
    val eventhubSharedAccessKeyName = appConfig.eventhubConfigMap.getOrElse("saskeyname", "")
    val eventhubSharedAccessKey = appConfig.eventhubConfigMap.getOrElse("saskey", "")

    // eventhub name
    val eventhubName = appConfig.eventhubConfigMap("name")

    val eventhubStoreOffsetsPath = appConfig.eventhubConfigMap("store-offsets-path")
    val eventhubAutoOffsetReset = appConfig.eventhubConfigMap("auto-offset-reset")
    val maxOffsetFiles = appConfig.eventhubConfigMap("max-offset-files").toInt
    val maxOffsetHistoryFiles = appConfig.eventhubConfigMap("max-offset-history-files").toInt

    // topic
    val topics: Array[String] = appConfig.eventhubConfigMap("topics").split(",")
    val topicJsonPath = appConfig.eventhubConfigMap.getOrElse("topic-json-path", "")

    // bstl expression to select sort field in incoming records
    //$.header.timestamp
    val sortDataBy: String = appConfig.eventhubConfigMap.getOrElse("sort-data-by", "")

    // max rate per partition
    val maxRatePerPartition: Int = appConfig.eventhubConfigMap.getOrElse("max-rate-per-partition", "1000").toInt

    // consumer group
    val eventhubConsumerGroup: String = appConfig.eventhubConfigMap.getOrElse("consumer-group", "$Default")

    // timeouts
    val receiverTimeout: Int = appConfig.eventhubConfigMap.getOrElse("receiver-timeout", "60").toInt
    val operationTimeout: Int = appConfig.eventhubConfigMap.getOrElse("operation-timeout", "60").toInt

    // retry attempts
    val retryMaxAttempts = appConfig.eventhubConfigMap.getOrElse("retry-attempts", "5").toInt
    val retryInterval = appConfig.eventhubConfigMap.getOrElse("retry-interval", "5000").toInt

    log.warn(s"""$runtimeApplicationId - ${LogTag.START_APP} -
      |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
      |message: starting application $applicationId $appName with $runtimeApplicationId"""
      .stripMargin.replaceAll("\n", " "))

    try {

      val eventHubOffsetSpecs = new EventHubOffsetSpecs(appName, eventhubStoreOffsetsPath)

      // get stored offsets
      val storedOffsets = RetryUtils.retry(retryMaxAttempts, retryInterval) {
        eventHubOffsetSpecs.getStoredOffsetsMap()
      }

      // event hub connection string
      val connectionString = if (eventhubEndpoint !="") ConnectionStringBuilder(eventhubEndpoint)
        .setEventHubName(eventhubName)
        .build else ConnectionStringBuilder().setNamespaceName(eventhubNamespace)
        .setSasKeyName(eventhubSharedAccessKeyName)
        .setSasKey(eventhubSharedAccessKey)
        .setEventHubName(eventhubName)
        .build

      // initialize EventHubsConf
      var ehConf:EventHubsConf = null
      if (storedOffsets.size > 0) {

        //start from stored offsets
        ehConf = EventHubsConf(connectionString)
          .setConsumerGroup(eventhubConsumerGroup)
          .setStartingPositions(storedOffsets)
          .setMaxRatePerPartition(maxRatePerPartition)
          .setReceiverTimeout(Duration.ofSeconds(receiverTimeout))
          .setOperationTimeout(Duration.ofSeconds(operationTimeout))

      } else {

        //start from specified auto-offset-reset
        val eventPos: EventPosition =
          if (eventhubAutoOffsetReset == "smallest" || eventhubAutoOffsetReset == "earliest")
            EventPosition.fromStartOfStream
          else
            EventPosition.fromEndOfStream

        ehConf = EventHubsConf(connectionString)
          .setConsumerGroup(eventhubConsumerGroup)
          .setStartingPosition(eventPos)
          .setMaxRatePerPartition(maxRatePerPartition)
          .setReceiverTimeout(Duration.ofSeconds(receiverTimeout))
          .setOperationTimeout(Duration.ofSeconds(operationTimeout))
      }

      val ssc = new StreamingContext(sparkContext, Seconds(duration))

      //create a direct stream
      val stream = EventHubsUtils.createDirectStream(ssc, ehConf)

      // obtain offset information for each partition that was read from Eventhub
      val messagesWithOffsetRanges = stream.transform { (rdd, batchTime) =>

        // batchTime str
        val batchTimeMillisecs = batchTime.milliseconds
        val batchTimeStr = SparkUtils.formatBatchTime(batchTimeMillisecs)

        //reset offsetArray
        var offsetSpecArray = ArrayBuffer.empty[EventHubOffsetSpec]

        // first method invoked on InputDStream, in order to get offsets
        val offsetRanges= rdd.asInstanceOf[EventHubHasOffsetRanges].offsetRanges


        offsetRanges.foreach(offsetRange => {
          log.info(s"$appName: found offset: ${offsetRange.toString}")
          offsetSpecArray += new EventHubOffsetSpec(appName, batchTimeStr, offsetRange)
        })

        // log offsets
        var topicTotalCount = MutableMap[String, Long]()
        for (offsetSpec <- offsetSpecArray) {
          val offsetRange = offsetSpec.offsetRange
          val (topic: String, partition: Int,
            fromOffset: Long, untilOffset: Long,
            count: Long) = (offsetRange.name, offsetRange.partitionId,
              offsetRange.fromSeqNo, offsetRange.untilSeqNo,
              offsetRange.count)
          if (count > 0) {
            log.warn(s"""$runtimeApplicationId - ${LogTag.SOURCE_TOPIC_PARTITION_COUNT} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
              |topic: $topic, partition: $partition, from: $fromOffset, to: $untilOffset, count: $count,
              |message: found $count records for topic $topic and partition $partition"""
              .stripMargin.replaceAll("\n", " "))
          }

          // update topic total count
          val oldTopicCount: Long = topicTotalCount.getOrElse(topic, 0)
          topicTotalCount(topic) = oldTopicCount + count
        }

        //log total counts
        var sourceAllTotal: Long = 0
        for ( (topic, count) <- topicTotalCount) {
          if (count > 0) {
            log.warn(s"""$runtimeApplicationId - ${LogTag.SOURCE_TOPIC_COUNT} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
              |topic: $topic, count: $count,
              |message: found $count records for topic $topic"""
              .stripMargin.replaceAll("\n", " "))
            sourceAllTotal = sourceAllTotal + count
          }
        }

        // extract topic and source data json
        val topicJson = rdd.map(x => {
          val sourceStr = new String(x.getBytes(), Charset.forName("UTF-8"))
          val topicStrVal: String = if (topicJsonPath.contains("$") ) {
            // extract topic from the source data
            Try {
              //extract topic
              val jobj = (new ObjectMapper).readValue(sourceStr, classOf[Object])
              val qr = JsonPath.query(topicJsonPath, jobj)
              qr match {
                case Left(x) => "unknown"
                case Right(x) => {
                  val item0 = qr.right.map(_.toVector).right.get
                  if (item0.length > 0 ) item0(0)
                }
              }
            } match {
              case Success(topicStrVal) => topicStrVal.toString
              case Failure(ex) => {

                val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)
                val contentStr = LogUtils.convToJsonString(sourceStr)

                log.error(s"""$runtimeApplicationId - ${LogTag.TRANSFORM_DATA_FAILED} -
                |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
                |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
                |sourceRecord: $contentStr,
                |action: discarded,
                |message: error extracting topic $exMesg"""
                .stripMargin.replaceAll("\n", " "))

                "unknown"
              }
            }
          } else {
            // literal
            topicJsonPath
          }
          (topicStrVal, sourceStr, offsetSpecArray, sourceAllTotal)
        })
        topicJson
     }

      // get storage level
      val defaultStorageLevelStr = appContext.appConfig.appConfigMap.getOrElse(
        "default-storage-level",
        "")
      val defaultStorageLevel = SparkUtils.getStorageLevel(defaultStorageLevelStr)

      var encounteredException : Option[Exception] = None

      messagesWithOffsetRanges.foreachRDD( (rdd, batchTime) => {

        // batch start
        val startBatch = System.currentTimeMillis
        val runtimeApplicationId = appContext.appConfig.runtimeAppId

        // batchTime str
        val batchTimeMillisecs = batchTime.milliseconds
        val batchTimeStr = SparkUtils.formatBatchTime(batchTimeMillisecs)

        // cache rdd, to prevent fetching data from eventhub
        // again and again. This should cache the rdd after
        // fetching data from kafka for the first time.
        rdd.persist(defaultStorageLevel)

        // check of consumer record rdd has any elements
        if (rdd.isEmpty()) {
          log.warn(s"""$runtimeApplicationId - ${LogTag.SOURCE_EMPTY} -
            |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
            |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
            |message: found no records to process""".stripMargin.replaceAll("\n", " "))

        } else {

          val uow : UnitOfWork = UnitOfWork.createUnitOfWork()

          val (topic, content, offsetSpecArray, sourceAllTotal) = rdd.first

          //extract topic, content from RDD
          val topicContentRDD: RDD[(String, String)] = rdd.map(x => (x._1, x._2))

          val auditLog = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)

          //sort the rdd if necessary
          val sortedRDD =
            if (sortDataBy.length > 0)
              SparkUtils.sortDataDF(topicContentRDD, sortDataBy, batchTimeMillisecs, appContext)
            else
              topicContentRDD

          // sortedRDD.collect().foreach(rdd => log.info("TESTAAA: SORTED REC: " + rdd))

          //cache: persist sortedRDD
          sortedRDD.persist(defaultStorageLevel)

          val opts: Set[RddProcessorOpts] = Set(
            RddProcessorOpts.REPART_SORT)
          val rddProcessor = new RddProcessor(appContext, appName, opts)
          encounteredException = rddProcessor.processRDD(uow, batchTimeMillisecs, sortedRDD, auditLog)

          //
          if (encounteredException.isDefined) {
            appContext.hdfsAuditLogger.logException(encounteredException.get)
            //abort the job
            throw new Exception(encounteredException.get)
          }

          //wite the raw records to hdfs
          val appConfig = appContext.appConfig

          //get store raw configuration
          val sparkSession = SparkSession.builder.getOrCreate()
          val sqlContext = sparkSession.sqlContext

          val storeRawEnabled = appConfig.storeRawConfigMap.getOrElse("enabled", "false")
          val storeRawPath = appConfig.storeRawConfigMap.getOrElse("path", "")
          val storeRawFormat = appConfig.storeRawConfigMap.getOrElse("format", "")
          val storeRawMapping: ImmutableMap[String, String] = appConfig.storeRawMappingConfigMap

          encounteredException = StoreRawUtils.storeRawData(
            appContext,
            batchTimeMillisecs,
            topics,
            sqlContext,
            storeRawEnabled,
            storeRawPath,
            storeRawFormat,
            storeRawMapping,
            sortedRDD)

          // cache: remove sortedRDD
          sortedRDD.unpersist()

          if (encounteredException.isDefined) {
            appContext.hdfsAuditLogger.logException(encounteredException.get)
            throw new Exception(encounteredException.get)
          }

          // store offsets
          try {

            log.info(s"$appName : about to eventhub commit offsets to $eventhubStoreOffsetsPath...")

            val storeSpecs = new EventHubOffsetSpecs(appName, eventhubStoreOffsetsPath)

            //get last saved offsets
            val storedOffsetsArray = RetryUtils.retry(retryMaxAttempts, retryInterval) {
              eventHubOffsetSpecs.getStoredOffsets()
            }

            // validate new offsets before save
            val diffMap = storeSpecs.compareOffsetsBeforeSave(storedOffsetsArray, offsetSpecArray)

            if (diffMap.size > 0) {

              for ((nameAndPartition, reason) <- diffMap) {
                log.error(s"""$runtimeApplicationId - ${LogTag.STORE_OFFSETS} -
                  |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                  |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
                  |topic: $topic, storeOffsetsPath: $eventhubStoreOffsetsPath,
                  |message: last saved offset mismatch for $nameAndPartition with reason $reason"""
                  .stripMargin.replaceAll("\n", " "))
              }
              throw new Exception("Last saved offsets do not match new offsets, going to bail")
            }

            val storeOffsetResult = RetryUtils.retry(retryMaxAttempts, retryInterval) {
              storeSpecs.storeOffsets(offsetSpecArray, maxOffsetFiles, maxOffsetHistoryFiles)
            }

            log.warn(s"""$runtimeApplicationId - ${LogTag.STORE_OFFSETS} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
              |topic: $topic, storeOffsetsPath: $eventhubStoreOffsetsPath,
              |message: finished storing offsets at $eventhubStoreOffsetsPath"""
              .stripMargin.replace("\n", " "))

            log.info(s"$appName: finished committing offsets to $eventhubStoreOffsetsPath")

            log.info(s"$appName: about to log EventHub offsets to the audit logs: new: count={" + offsetSpecArray.size.toString() + "}...")

            //going to log the offsets
            val auditLogger = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)
            offsetSpecArray.foreach(o => {
              val offsetRangeJSON = EventHubOffsetSpec.toJSONString(o)
              auditLogger.insertLogMessage(s"$uow: " + offsetRangeJSON)

            })
            auditLogger.flush()
            log.info(s"$appName: done logging eventhub offsets to the audit logs...")

            val durationBatch = System.currentTimeMillis - startBatch

            log.warn(s"""$runtimeApplicationId - ${LogTag.BATCH_DURATION} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
              |duration: $durationBatch, count: $sourceAllTotal,
              |message: batch processed $sourceAllTotal records in $durationBatch milliseconds"""
              .stripMargin.replaceAll("\n", " "))

          } catch {

            case ex : Exception => {

              val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

              log.error(s"""$runtimeApplicationId - ${LogTag.STORE_OFFSETS} -
                |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
                |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
                |message: error saving offsets""".stripMargin
                .replaceAll("\n", " "))

                throw new Exception(ex.getMessage)
              }
          }
        }// else isEmpty

        // done processing data, now remove rdd from cache
        // so we dont run out memory
        rdd.unpersist()

      })

      // start streaming context
      ssc.start()


      // wait for streaming context to terminate
      ssc.awaitTermination()


    } catch {

      case ex: Throwable => {

        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""$runtimeApplicationId - ${LogTag.FATAL_APP_EXIT_ERROR} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
          |message: bailing out have run into fatal driver error with exception $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        log.error(s"""$runtimeApplicationId - ${LogTag.STOP_APP} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |message: application $appName with applicationId $applicationId stopped"""
          .stripMargin.replaceAll("\n", " "))

        appContext.hdfsAuditLogger.logThrowableException(ex)

        // kill the driver
        System.exit(50)
      }
    }

  }

}
