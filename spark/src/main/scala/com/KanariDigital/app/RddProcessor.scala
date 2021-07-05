/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app

import scala.collection.mutable.{ArrayBuffer => MutableArrayBuffer}
import scala.util.{Failure, Success, Try}
import com.KanariDigital.Orderbook.HdfsHiveWriter.OrderBookContext
import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, UnitOfWork}
import com.KanariDigital.app.util.{ContextUtils, EventhubWriteUtils,
  HBaseWriteUtils, JdbcWriteUtils, HdfsWriteUtils, LogTag, SparkUtils}
import com.kanaridi.common.util.LogUtils
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.kanaridi.xform.{HBRow, MessageMapper, TransformResult}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD


/** partitioner for hbrows
  *
  * hbrows belonging to a table and row key are assigned same partition.
  *
  * Used to repartition hbrows when [[RddProcessorOpts.REPART_SORT]] is set
  */
class HBRowPartitioner(override val numPartitions: Int) extends Partitioner {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  require(numPartitions >= 0, s"Number of partitions ($numPartitions) cannot be negative.")

  def getPartition(key: Any): Int = {
    val hbrowKey = key.asInstanceOf[HBRowKey]
    val (tableName, rowKey) = (hbrowKey.tableName, hbrowKey.rowKey)
    val partStr = s"$tableName-$rowKey"
    val partHash = Math.abs(partStr.hashCode())
    val partitionNo = (partHash % numPartitions).toInt
    // log.warn(s"NAME $partStr HASH $partHash GETPARTITION $partitionNo")
    partitionNo
  }
}


/** key to sort hbrows
  *
  * hbrows in a partition are sorted by table name,
  * row key and sort key
  *
  * Used to sort hbrows within a partition when
  * [[RddProcessorOpts.REPART_SORT]] is set
  */
case class HBRowKey(tableName: String, rowKey: String, sortKey: String)

object HBRowKey {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  implicit def orderingBySortKey[T <: HBRowKey] : Ordering[T] = {
    Ordering.by(fk => {
      val hbrowKey = fk.asInstanceOf[HBRowKey]
      // implicit sort
      // sort by tablename, rowkey and sortkey
      (hbrowKey.tableName, hbrowKey.rowKey, hbrowKey.sortKey)
    })
  }
}

/** RddProcessor flags
  *
  * REPART_SORT    enable sorting of partioning hbrows by
  *                    tablename, rowkey and sort by sort key
  */
sealed trait RddProcessorOpts
object RddProcessorOpts {
  case object NO_OPT extends RddProcessorOpts
  case object REPART_SORT extends RddProcessorOpts
}

import RddProcessorOpts._


class RddProcessor(val appContext: AppContext, val appName: String, val opts: Set[RddProcessorOpts])
    extends Serializable {

  // import flags
  import RddProcessorOpts._

  /** Data types */
  val ORDERBOOK_DATA = "orderbook"
  val LOGISTICS_DATA = "logistics"
  val HBROW_DATA  = "hbrow"

  val defaultStorageLevelStr = appContext.appConfig.appConfigMap.getOrElse(
    "default-storage-level",
    "MEMORY_AND_DISK_SER")

  val defaultStorageLevel = SparkUtils.getStorageLevel(defaultStorageLevelStr)

  // default number of partitions used when repartitioning/sorting hbrows
  val defaultShufflePartitions = "200"

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** contructor to create RddProcessor with default options
    */
  def this(appContext: AppContext, appName: String) {
    this(appContext, appName, Set(NO_OPT))
  }

  /** constructor to use OrderbookContext to create a RddProcessor */
  def this(appName: String, orderBookContext: OrderBookContext) {
    this(ContextUtils.orderBook2AppContext(appName, orderBookContext), appName)
  }

  /** transform the source json, and get transformed result */
  def transformMapping(messageMapper:MessageMapper,
    rdd: RDD[(String, String)],
    batchTime: Long = 0L): RDD[TransformResult] = {

    log.info(s"RDD_TRACE $batchTime transformMapping: start...")

    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)
    val runtimeApplicationId = appContext.appConfig.runtimeAppId
    val applicationId = appContext.appConfig.getApplicationId
    val appName = appContext.appConfig.appName
    val appTypeStr = appContext.appConfig.appType.toString

    //val start = System.currentTimeMillis
    //map operation to extract and transform
    val mappedResult = rdd.flatMap(t => {

      val topic = t._1
      val json = t._2

      var trArray = MutableArrayBuffer.empty[TransformResult]

      // perform data transformation
      Try { messageMapper.transform(topic, json) } match {
        case Success(x) => {
          // log.warn(s"source topic: $topic, source json: $json, result:{$x}")
          trArray ++= x
        }
        case Failure(ex) => {
          val (mesg, stackTrace) = LogUtils.getExceptionDetails(ex)
          val src = LogUtils.convToJsonString(json)
          log.error(s"""$runtimeApplicationId - ${LogTag.TRANSFORM_DATA_FAILED} -
            |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
            |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
            |record: $src,
            |action: discarded,
            |message: error in transform mapping $mesg"""
            .stripMargin.replaceAll("\n", " "))
        }
      }
      trArray.toList.iterator
    })
    //val duration = System.currentTimeMillis - start
    log.info(s"RDD_TRACE $batchTime transformMapping: done.")
    mappedResult
  }

  /** extract hbrows from source json
    * convert source RDD[(String, String)] to RDD[HBRow]
    */
  def convertToHBRows(rdd: RDD[(String, String)],
    batchTime: Long = 0L): RDD[HBRow] = {

    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)
    val runtimeApplicationId = appContext.appConfig.runtimeAppId
    val applicationId = appContext.appConfig.getApplicationId
    val appName = appContext.appConfig.appName
    val appTypeStr = appContext.appConfig.appType.toString

    val convertedHBRows = rdd.flatMap(x => {

      val (topic, hbrowStr) = x

      var hbrowsArray = MutableArrayBuffer.empty[HBRow]

      Try {

        // parse hbrow json
        val jsonMapper = new ObjectMapper
        jsonMapper.registerModule(DefaultScalaModule)
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        val hbrow: HBRow = jsonMapper.readValue(hbrowStr, classOf[HBRow])
        hbrow

      } match {

        case Success(hbr) => {
          hbrowsArray += hbr
        }
        case Failure(ex) => {

          val (mesg, stackTrace) = LogUtils.getExceptionDetails(ex)
          val src = LogUtils.convToJsonString(hbrowStr)
          log.error(s"""$runtimeApplicationId - ${LogTag.TRANSFORM_DATA_FAILED} -
            |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
            |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
            |record: $src,
            |action: discarded,
            |message: error in extracting hbrows $mesg"""
            .stripMargin.replaceAll("\n", " "))
        }
      }
      hbrowsArray.toList.iterator
    })
    convertedHBRows
  }

  /** repartition and sort hbrows
    *
    * extract hbrows repartition based on tableName and rowKey
    * and sort hbrow records within each partition by sort key
    *
    * @return RDD[HBRow]
    *         hbrows are partitioned by tablename, rowkey and
    *         records within a partitions are sorted
    *         by sortkey
    */
  def doRepartAndSort(
    batchTime: Long,
    hbrowsRDD: RDD[HBRow]): RDD[HBRow] = {

    //create a key value rdditer
    val hbrowRDD: RDD[(HBRowKey, HBRow)] = hbrowsRDD.map(rec => {

      val hbrow: HBRow = rec.asInstanceOf[HBRow]
      val tableName = hbrow.tableName
      val rowKey = hbrow.rowKey
      val sortKeyVal: String = hbrow.sortKey.toString

      new Tuple2(new HBRowKey(tableName, rowKey, sortKeyVal), hbrow)
    })

    // get property spark.shuffle.partitions for HBRowPartitioner
    val numPartitions = SparkUtils.getSparkConfProperty("spark.sql.shuffle.partitions", defaultShufflePartitions).toInt

    // part and sort
    val partSorted = hbrowRDD.repartitionAndSortWithinPartitions(new HBRowPartitioner(numPartitions))

    // map does not change the order
    val hbrowsSorted = partSorted.map(x => {
      val (key, hbrow) = x
      hbrow
    })

    hbrowsSorted
  }

  /** extract hbrows from transform results
    *
    * extract hbrows repartition based on tableName and rowKey
    * and sort hbrow records within each partition by sort key
    *
    * @return RDD[HBRow]
    */
  def getHBRows(
    batchTime: Long,
    transformResultRDD: RDD[TransformResult]): RDD[HBRow] = {

    //create a key value rdditer
    val hbrows: RDD[HBRow] = transformResultRDD.flatMap(transformResult => {
      var hbrowsArray = MutableArrayBuffer.empty[HBRow]
      transformResult.hbaseRows.foreach(rec => {
        val hbrow: HBRow = rec.asInstanceOf[HBRow]
        hbrowsArray += hbrow
      })
      hbrowsArray.toList.iterator
    })
    hbrows
  }

  /** write idocids from {{ RDD[TransformResult] }} to the audit log
    *
    * @return number of distinct idocs recorded
    */
  def writeIDocIdsToAuditLog(appContext: AppContext,
    batchTime: Long,
    uow: UnitOfWork,
    mappedResult: RDD[TransformResult],
    auditLog: CompositeAuditLogger): Unit = {

    // format batch time
    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)
    val runtimeApplicationId = appContext.appConfig.runtimeAppId
    val applicationId = appContext.appConfig.getApplicationId
    val appTypeStr = appContext.appConfig.appType.toString

    // app
    val appConfig = appContext.appConfig
    val appConfigMap = appConfig.appConfigMap
    val enableLogIDoc = appConfigMap.getOrElse("enabled-log-idoc", "true")

    Try {

      // write to eventhub if enabled
      if (enableLogIDoc == "true") {

        val topicIdocIdSet: RDD[(String, String)] = mappedResult.flatMap(oneResult => {
          // add (topic, docIds) tuple to Set
          val topicIDocIds: Set[(String, String)] = oneResult.docIDs.map(x => (oneResult.topic, x))
          topicIDocIds
        })
        topicIdocIdSet.persist(defaultStorageLevel)

        val topicIdocIds =  topicIdocIdSet.collect()

        topicIdocIds.foreach(x => {

          val (topic, idocId) = (x._1, x._2)

          // IMPORTANT: dont change this format
          auditLog.insertLogMessage(s"[${uow.toString()}] IDocId: $idocId")

          log.info(s"""$runtimeApplicationId - ${LogTag.TRANSFORMED_TOPIC_IDOC} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |topic: $topic, iDocId: $idocId,
          |message: found $idocId  with topic $topic"""
            .stripMargin.replaceAll("\n", " "))

        })

        // audit log flush
        auditLog.flush()

        // rdd action
        val distinctTopicIDocIdCount = topicIdocIds.length.toLong

        log.info(s"RDD_TRACE $batchTime counted $distinctTopicIDocIdCount in idocIds")
        log.info(s"Writing to Audit Log, Distinct IDoc Ids: = $topicIdocIdSet")

        log.warn(s"""$runtimeApplicationId - ${LogTag.TRANSFORMED_IDOC_COUNT} -
        |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
        |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
        |count: $distinctTopicIDocIdCount,
        |message: found $distinctTopicIDocIdCount idocs"""
          .stripMargin.replaceAll("\n", " "))

        // unpersist
        topicIdocIdSet.unpersist()

        distinctTopicIDocIdCount

      } else {
        log.info(s"$appName skip writing data to log idoc flag is set to $enableLogIDoc...")
      }

    } match {
      case Success(x) => {
        // nothing to return
      }
      case Failure(ex) => {
        // ex.printStackTrace()
        // failed
        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_IDOC} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
          |message: error storing data to eventhub $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        throw ex
      }
    }
  }

  /** write transformed results to hbase, eventhub, sql and hdfs */
  def writeResultsRepartAndSort(
    appContext: AppContext,
    hbrowsResult: RDD[HBRow],
    auditLog: CompositeAuditLogger,
    uow: UnitOfWork,
    batchTime: Long): Unit = {

    // format batch time
    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)
    val runtimeApplicationId = appContext.appConfig.runtimeAppId
    val applicationId = appContext.appConfig.getApplicationId
    val appTypeStr = appContext.appConfig.appType.toString

    try {

      val start = System.currentTimeMillis

      log.info(s"RDD_TRACE $batchTime writeResultsRepartAndSort: start")

      log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_RESULT} -
            |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
            |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
            |message: write results repart and sort start""".stripMargin.replaceAll("\n", " "))

      // repartition and sort within partitions
      val hbrows: RDD[HBRow] = if (opts contains REPART_SORT) {
        doRepartAndSort(batchTime, hbrowsResult)
      } else {
        hbrowsResult
      }

      // persist
      hbrows.persist(defaultStorageLevel)

      if (hbrows.isEmpty()) {

        log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_RESULT} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |message: found no records to process"""
                    .stripMargin.replaceAll("\n", " "))

      } else {
        val numRows = hbrows.count // force rdd evaluation

        log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_RESULT} -
            |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
            |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
            |message: write results repart and sort completed for $numRows potential inserts, starting write""".stripMargin.replaceAll("\n", " "))


        // insert hbrows into hbase
        // total count of hbrows written to hbase
        val totalCountHBase = HBaseWriteUtils.writeResultsToHBase(appContext, batchTime,
          hbrows)

        // insert hbrows into jdbc specifically for sql2019
        // total count of hbrows written to sql
        val totalCountJdbc = JdbcWriteUtils.writeResultsToJdbc(appContext, batchTime,
          hbrows)

        // write hbrows to eventhub
        // total count of hbrows written to eventhub
        val totalCountEventhub = EventhubWriteUtils.writeResultsToEventhub(appContext, batchTime,
          hbrows)

        // write hbrows to eventhub
        // total count of hbrows written to hdfs
        HdfsWriteUtils.writeResultsToHdfs(appContext, batchTime,
          hbrows)

      }

      // partitioned and sorted data
      hbrows.unpersist()

      val duration = System.currentTimeMillis - start
      log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_RESULT} -
        |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
        |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
        |duration: $duration,
        |message: write to hbase done in $duration seconds"""
        .stripMargin.replaceAll("\n", " "))

      log.info(s"RDD_TRACE $batchTime writeResultsRepartAndSort: done in $duration milliseconds")


    } catch {

      case ex: Exception => {

        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_RESULT} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
          |message: write results failed $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        throw ex
      }
    }
  }

  /**  transform source data, using mapper and write results to hbase
    *  - transform source data RDD[(String, String)] using a mapper set via appContext
    *  - write transformed data (which is series of hbase insert statements) to hbase
    *  - logs the topic and docIds info to audit log and spark logs
    *  returns count of unique iDocIds
    *  @param rdd source data
    *  @param audit logger, write topic and docIds processed information to audit log
    *  returns Option[Exception] on failure
    */
  def processRDD(uow: UnitOfWork, rdd: RDD[(String, String)],
    auditLog: CompositeAuditLogger): Option[Exception] = {

    // intialize batchTime to current time
    val now = System.currentTimeMillis()

    processRDD(uow, now, rdd, auditLog)
  }

  /**  transform source data, using mapper and write results to hbase
    *  - transform source data RDD[(String, String)] using a mapper set via appContext
    *  - write transformed data (which is series of hbase insert statements) to hbase
    *  - logs the topic and docIds info to audit log and spark logs
    *  returns count of unique iDocIds
    *  @param rdd source data
    *  @param batchTime spark batch time
    *  @param auditLog logger, write topic and docIds processed information to audit log
    *  returns Option[Exception] on failure
    */
  def processRDD(uow: UnitOfWork, batchTime: Long, rdd: RDD[(String, String)],
    auditLog: CompositeAuditLogger): Option[Exception] = {

    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)
    val runtimeApplicationId = appContext.appConfig.runtimeAppId

    val appConfig = appContext.appConfig
    val applicationId = appConfig.getApplicationId
    val appTypeStr = appConfig.appType
    val appName = appConfig.appName

    // data type
    val dataType = appConfig.appConfigMap.getOrElse("data-type", "")

    val start = System.currentTimeMillis

    try {

      log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD} -
            |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
            |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
            |message: process rdd start""".stripMargin.replaceAll("\n", " "))

      // extract hbrow info
      if (dataType == HBROW_DATA) {

        // start
        val startWrite = System.currentTimeMillis

        // start
        log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_HBROWS_WRITE} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |message: hbrows write start"""
          .stripMargin.replaceAll("\n", " "))

        // parse hbrows
        val hbrows: RDD[HBRow] = convertToHBRows(rdd, batchTime)

        writeResultsRepartAndSort(appContext, hbrows,
          auditLog, uow, batchTime)

        val durationWrite = System.currentTimeMillis - startWrite
          log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_HBROWS_WRITE} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |duration: $durationWrite,
          |message: hbrows write done in $durationWrite seconds"""
          .stripMargin.replaceAll("\n", " "))

      } else {

        // to xform
        val messageMapper = appContext.xform
        val transformMappingResult: RDD[TransformResult] = transformMapping(messageMapper, rdd,
          batchTime)

        // cache transformed results
        transformMappingResult.persist(defaultStorageLevel)

        if (transformMappingResult.isEmpty()) {

          log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_TRANSFORM_WRITE} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |message: found no records to process"""
                    .stripMargin.replaceAll("\n", " "))

        } else {

          // start
          val startWrite = System.currentTimeMillis

          // start
          log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_TRANSFORM_WRITE} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |message: transform and write start"""
          .stripMargin.replaceAll("\n", " "))

          val hbrows = getHBRows(batchTime, transformMappingResult)

          writeResultsRepartAndSort(appContext, hbrows,
            auditLog, uow, batchTime)

          // log idocs in incoming data
          writeIDocIdsToAuditLog(appContext, batchTime,
            uow,
            transformMappingResult,
            auditLog)

          val durationWrite = System.currentTimeMillis - startWrite

          log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_TRANSFORM_WRITE} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |duration: $durationWrite,
          |message: transform and write done in $durationWrite seconds"""
          .stripMargin.replaceAll("\n", " "))
        }

        transformMappingResult.unpersist()

      }

    } catch {

      case ex: Exception => {

        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
          |message: transform write exception $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        return Some(ex)
      }
    }
    log.info(s"RDD_TRACE $batchTime Completed processing RDD")

    val duration = System.currentTimeMillis - start

    log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_TRANSFORM_WRITE} -
      |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
      |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
      |duration: $duration,
      |message: process rdd done in $duration milliseconds"""
      .stripMargin.replaceAll("\n", " "))
    None
  }
}
