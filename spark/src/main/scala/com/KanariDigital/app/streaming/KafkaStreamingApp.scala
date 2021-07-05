/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.streaming

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.{Map => ImmutableMap}
import scala.collection.mutable.{Map => MutableMap}

import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, HdfsLogger, UnitOfWork}

import com.KanariDigital.app.{App, AppType, AppContext, RddProcessor, RddProcessorOpts}

import com.KanariDigital.app.util.{LogTag, SparkUtils, StoreRawUtils}
import com.kanaridi.common.util.{LogUtils, RetryUtils}

import org.apache.commons.logging.{Log, LogFactory}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.Duration

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies,
  CanCommitOffsets, HasOffsetRanges, OffsetRange}

/** Kafka spark streaming app which retrieves data from kafka,
  * transforms the data and writes the data into one or more hbase tables
  */
class KafkaStreamingApp(appName: String) extends
    App(appName, AppType.KAFKA_STREAMING_APP_TYPE) {

  override val appTypeStr = AppType.KAFKA_STREAMING_APP_TYPE

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** run kafka application which processes raw data from kafka
    * @param isLocal - set to true to create a local spark streaming context
    * @param appContext - application context which contains,
    *                     [[MessageMapperFactory]]  and [[AppConfig]] objects
    */
  override def runApp(appContext: AppContext, isLocal: Boolean): Unit = {

    log.info(s"$appName: runApp: start")

    // create spark context from spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession: SparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)
    val sparkContext = sparkSession.sparkContext

    // log4j
    // val rootLogger = Logger.getRootLogger()
    // rootLogger.setLevel(Level.WARN)
    // Logger.getLogger("org").setLevel(Level.WARN)
    // Logger.getLogger("akka").setLevel(Level.WARN)

    //sqlContext
    val sqlContext = sparkSession.sqlContext

    // runtime application id
    val runtimeApplicationId = sparkContext.applicationId
    appContext.appConfig.runtimeAppId = runtimeApplicationId


    val appConfig = appContext.appConfig

    val duration = appConfig.sparkConfigMap("batchDuration").toLong
    log.info(s"$appName: runApp: Creating Spark Streaming Context with batch duration of $duration seconds")

    // set log level
    val logLevel = appConfig.appConfigMap.getOrElse("log-level", "INFO")
    sparkContext.setLogLevel(logLevel)

    // app id
    val applicationId = appConfig.getApplicationId

    // get kafka configuration

    //topic
    val topics: Array[String] = appConfig.kafkaConfigMap("topics").split(",")
    val topicJsonPath = appConfig.kafkaConfigMap.getOrElse("topic-json-path", "")

    //group id
    val groupId = appConfig.kafkaConfigMap("group.id")
    val keyDeserializer = appConfig.kafkaConfigMap("key.deserializer")
    val valueDeserializer = appConfig.kafkaConfigMap("value.deserializer")
    val brokers = appConfig.kafkaConfigMap("brokers")
    val autoOffsetReset = appConfig.kafkaConfigMap("auto.offset.reset")
    val securityProtocol = appConfig.kafkaConfigMap("security.protocol")
    val enableAutoCommit = appConfig.kafkaConfigMap("enable.auto.commit")
    val sessionTimeout = appConfig.kafkaConfigMap("session.timeout")
    val heartbeatInterval = appConfig.kafkaConfigMap("heartbeat.interval")

    //ssl
    val sslTruststoreLocation = appConfig.kafkaConfigMap.getOrElse("ssl.truststore.location", "")
    val sslTruststorePassword = appConfig.kafkaConfigMap.getOrElse("ssl.truststore.password", "")
    val sslKeystoreLocation = appConfig.kafkaConfigMap.getOrElse("ssl.keystore.location", "")
    val sslKeystorePassword = appConfig.kafkaConfigMap.getOrElse("ssl.keystore.password", "")
    val sslKeyPassword = appConfig.kafkaConfigMap.getOrElse("ssl.key.password", "")

    //zookeeper?
    val zookeeperConnect = appConfig.kafkaConfigMap.getOrElse("zookeeper.connect", "")

    log.warn(s"""$runtimeApplicationId - ${LogTag.START_APP} -
      |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
      |message: starting application $applicationId $appName with runtimeApplicationId $runtimeApplicationId"""
      .stripMargin.replaceAll("\n", " "))

    try {

      val ssc = new StreamingContext(sparkContext, Seconds(duration))

      // start kafka app
      val directStreamProperties = Map(
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> keyDeserializer,
        "value.deserializer" -> valueDeserializer,
        "group.id" -> groupId,
        "auto.offset.reset" -> autoOffsetReset,
        "security.protocol" -> securityProtocol,
        "ssl.truststore.location" -> sslTruststoreLocation,
        "ssl.truststore.password" -> sslTruststorePassword,
        "ssl.keystore.location" -> sslKeystoreLocation,
        "ssl.keystore.password" -> sslKeystorePassword,
        "ssl.key.password" -> sslKeyPassword,
        "zookeeper.connect" -> zookeeperConnect,
        "enable.auto.commit" -> enableAutoCommit,
        "session.timeout.ms" -> sessionTimeout,
        "heartbeat.interval.ms" -> heartbeatInterval)

      val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, directStreamProperties))

      var offsetArray = ArrayBuffer.empty[OffsetRange]
      var uow: UnitOfWork = null
      // obtain offset information for each partition that was read from kafka
      // and write to hdfs audit log
      val messagesWithOffsetRanges = kafkaDirectStream.transform { rdd =>
        try {
          // first method invoked on InputDStream, in order to get offsets
          val r = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetArray ++= r

          uow = UnitOfWork.createUnitOfWork()

        } catch {

          case ex: Exception => {

            val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

            log.error(s"""$runtimeApplicationId - ${LogTag.APP_ERROR} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
              |message: error reading offsets skipping offsets"""
              .stripMargin.replaceAll("\n", " "))
          }
        }
        rdd
      }

      // extract ConsumerRecord(String, String) key and value pairs
      // value is json format
      val topicAndJson = messagesWithOffsetRanges.map(cr => (cr.topic(), cr.value()))

      var encounteredException : Option[Exception] = None
      topicAndJson.foreachRDD((rdd, batchTime) => {

        // batchTime str
        val startBatch = System.currentTimeMillis
        val batchTimeMillisecs = batchTime.milliseconds
        val batchTimeStr = SparkUtils.formatBatchTime(batchTimeMillisecs)

        var topicTotalCount = MutableMap[String, Long]()

        for (offsetRange <- offsetArray) {

          val (topic: String, partition: Int, fromOffset:
              Long, untilOffset: Long, count: Long) = (offsetRange.topic, offsetRange.partition,
                offsetRange.fromOffset, offsetRange.untilOffset, offsetRange.count)

          if (count > 0 ) {
            log.warn(s"""$runtimeApplicationId - ${LogTag.SOURCE_TOPIC_PARTITION_COUNT} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
              |topic: $topic, partition: $partition, from: $fromOffset, to: $untilOffset, count: $count,
              |message: found $count records for topic $topic and partition $partition"""
              .stripMargin.replaceAll("\n", " "))
          }

          // update total count
          val oldCount: Long = topicTotalCount.getOrElse(topic, 0)
          topicTotalCount(topic) = oldCount + count
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
        // if (heartBeat.isDefined) {
        //   val hb = heartBeat.get.signalAlive()
        // }

        // cache rdd, to prevent fetching data from kafka
        // again and again. This should cache the rdd after
        // fetching data from kafka for the first time.
        rdd.persist()

        // check of consumer record rdd has any elements
        if (rdd.isEmpty()) {

          log.warn(s"""$runtimeApplicationId - ${LogTag.SOURCE_EMPTY} -
            |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
            |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
            |message: found no records to process"""
            .stripMargin.replaceAll("\n", " "))

        } else {

          val auditLog = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)

          // do the data transformation
          val opts: Set[RddProcessorOpts] = Set(
            RddProcessorOpts.REPART_SORT)
          val rddProcessor = new RddProcessor(appContext, appName, opts)
          encounteredException = rddProcessor.processRDD(uow, batchTimeMillisecs, rdd, auditLog)

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
            rdd)

          if (encounteredException.isDefined) {
            appContext.hdfsAuditLogger.logException(encounteredException.get)
            throw new Exception(encounteredException.get)
          }

          // store offsets
          try {

            log.info("About to commit offsets...")
            kafkaDirectStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetArray.toArray)
            log.info("Finished committing offsets.")

            log.info("About to log offsets to the audit logs...")
            //going to log the offsets
            //val auditLogger = CompositeAuditLogger.createHdfsAndLogAuditLogger(orderBookContext.hdfsAuditLoggerConfig)
            //auditLogger.logKafkaOffsetRanges(uow, offsetArray)
            //auditLogg.flush()
            log.info("Done logging offsets to the audit logs.")

            //reset offsetArray
            offsetArray = ArrayBuffer.empty[OffsetRange]

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
                |message: error saving offsets"""
                .stripMargin.replaceAll("\n", " "))

              appContext.hdfsAuditLogger.logException(ex)
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
          |message: application $appName with runtimeApplicationId $runtimeApplicationId stopped"""
          .stripMargin.replaceAll("\n", " "))

        appContext.hdfsAuditLogger.logThrowableException(ex)

        // kill the driver
        System.exit(50)
      }
    }
  }
}
