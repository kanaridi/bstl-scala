/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.util

import java.util.concurrent.{Executors, ScheduledExecutorService}

import scala.collection.mutable.{ArrayBuffer => MutableArrayBuffer}
import scala.util.{Failure, Success, Try}

import com.KanariDigital.app.AppContext

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.kanaridi.xform.{HBRow, TransformResult}

import com.microsoft.azure.eventhubs.{BatchOptions, ConnectionStringBuilder, EventData,
  EventDataBatch, EventHubClient, PayloadSizeExceededException}

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.rdd.RDD
import com.kanaridi.common.util.LogUtils


class EventhubWriterPool(appContext: AppContext) extends Serializable {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  var eventhubWriterMap = Map.empty[String, EventhubWriter]

  def token(tableName : String) : String = tableName.trim

  private def create(tableName: String) : EventhubWriter = {

    //ehWrite write connection properties
    val appConfig = appContext.appConfig

    val eventhubWriteConfigMap = appConfig.eventhubWriteConfigMap

    //  max batch size in bytes 256K
    //  val eventhubWriteMaxBatchSize = eventhubWriteConfigMap.getOrElse("max-batch-bytes",
    //   "262144").toInt

    val eventhubWriteMappingConfigMap = appConfig.eventhubWriteMappingConfigMap

    val ehConfig = eventhubWriteMappingConfigMap.getOrElse(tableName, Map[String, String]())

    val eventhubWriteEndpoint = ehConfig.getOrElse("endpoint", "")
    val eventhubWriteNamespace = ehConfig.getOrElse("namespace", "")
    val eventhubWriteName = ehConfig.getOrElse("name", "")
    val eventhubWriteSasKey = ehConfig.getOrElse("saskey", "")
    val eventhubWriteSasKeyName = ehConfig.getOrElse("saskeyname", "")

    // event hub connection string
    val ehConnection = if (eventhubWriteEndpoint !="")
      new ConnectionStringBuilder(eventhubWriteEndpoint)
        .setEventHubName(eventhubWriteName)
    else new ConnectionStringBuilder()
      .setNamespaceName(eventhubWriteNamespace)
      .setSasKeyName(eventhubWriteSasKeyName)
      .setSasKey(eventhubWriteSasKey)
      .setEventHubName(eventhubWriteName)

    val executorService:ScheduledExecutorService = Executors.newScheduledThreadPool(4)

    val ehClient: EventHubClient = EventHubClient.createSync(
      ehConnection.toString,
      executorService)

    val eventhubWriter = new EventhubWriter(ehClient)
    eventhubWriterMap += token(tableName) -> eventhubWriter
    eventhubWriter
  }

  def getOrCreate(tableName: String): EventhubWriter = {
    eventhubWriterMap.getOrElse(token(tableName), create(tableName))
  }

  def flush() : Unit = {
    log.warn("Flushing eventhub pool: " + eventhubWriterMap.map(x => x._1).mkString(", "))
    eventhubWriterMap.values.foreach(x => x.flush())
  }

  def close() : Unit = {
    log.warn("Closing eventhub pool: " + eventhubWriterMap.map(x => x._1).mkString(", "))
    eventhubWriterMap.values.foreach(x => x.close())
  }

}

/** eventhub writer */
class EventhubWriter(ehClient: EventHubClient) extends Serializable {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  // batch options
  val batchOptions: BatchOptions = new BatchOptions()

  // max size of batch in bytes
  // batchOptions.maxMessageSize = maxBatchSizeBytes

  var currentEhBatch: EventDataBatch = ehClient.createBatch()

  /** add event data to a batch returns false if there was an error or if batch size exceeds
    *  maximum batch size */
  private def addToBatch(currentEhBatch: EventDataBatch, eventData: EventData): Boolean = {

    // try adding event data to a batch
    var addToBatchResult = false

    try {
      addToBatchResult = currentEhBatch.tryAdd(eventData)
    } catch {
      case ex: PayloadSizeExceededException => {
        // return false if the eventData could not be
        // added to the batch
        return false
      }
    }
    addToBatchResult
  }

  def insertLines(hbrowJson: String) = {

    // prepare json for eventhub write
    val payloadBytes = hbrowJson.getBytes("UTF-8")

    val sendEvent: EventData = EventData.create(payloadBytes);
    // try adding event data to a batch
    val addBatchResult = addToBatch(currentEhBatch, sendEvent)

    // if adding sendEvent to a batch fails then
    // do a flush, create a new batch and add sendEvent
    // to the new batch
    if (addBatchResult == false) {

      if (currentEhBatch.getSize() > 0) {
        val batchSize = currentEhBatch.getSize
        log.warn(s"SENDING evhub insertLines batchSize: $batchSize going to send new batch")
        ehClient.sendSync(currentEhBatch)
        log.warn(s"SENDING evhub insertLines batchSize: $batchSize done sending.")
      }
      // create a new batch
      currentEhBatch = ehClient.createBatch()

      currentEhBatch.tryAdd(sendEvent)
    }
  }

  def flush() = {
    // flush any remaining items in batch
    if (currentEhBatch.getSize() > 0) {
      val batchSize = currentEhBatch.getSize
      ehClient.sendSync(currentEhBatch)
      log.warn(s"SENDING evhub flush batchSize: $batchSize flushed remaining.")
    }
  }

  def close() = {
    ehClient.closeSync()
  }

}


/** util to write contents of hbrows rdd to eventhub
  */
object EventhubWriteUtils extends Serializable {

  val log: Log = LogFactory.getLog(this.getClass.getName)


  /** write contents of {{ RDD[HBRow] }} to eventhub
    *
    * @return number of records written to eventhub
    */
  def writeResultsToEventhub(
    appContext: AppContext,
    batchTime: Long,
    hbrows: RDD[HBRow]): Long = {

    //ehWrite write connection properties
    val appConfig = appContext.appConfig

    val eventhubWriteConfigMap = appConfig.eventhubWriteConfigMap

    val eventhubWriteMappingConfigMap = appConfig.eventhubWriteMappingConfigMap

    // not enabled by default
    val eventhubWriteEnabled    = eventhubWriteConfigMap.getOrElse("enabled", "false")

    log.warn(s"eventhubWriteEnabled: $eventhubWriteEnabled")

    val eventhubWriteOperationTimeout = eventhubWriteConfigMap.getOrElse("operation-timeout",
      "60").toInt

    val defaultStorageLevelStr = appConfig.appConfigMap.getOrElse(
      "default-storage-level",
      "MEMORY_AND_DISK_SER")

    //app
    val applicationId = appConfig.getApplicationId
    val appName = appConfig.appName
    val appTypeStr = appConfig.appType
    val runtimeApplicationId = appConfig.runtimeAppId
    val batchTimeStr = SparkUtils.formatBatchTime(batchTime)

    // try
    Try {

      // write to eventhub if enabled
      if (eventhubWriteEnabled == "true") {

        val start = System.currentTimeMillis
        val defaultStorageLevel = SparkUtils.getStorageLevel(defaultStorageLevelStr)

        val tableCountList: RDD[(String, Int)]  = hbrows.mapPartitionsWithIndex((partIndex, rddIter) => {

          log.warn(s"SENDING evhub partIndex: {$partIndex} start")

          var tableCount = MutableArrayBuffer.empty[Tuple2[String, Int]]

          if (rddIter.hasNext) {

            // get a eventhub writer from the pool
            val eventhubWriterPool = new EventhubWriterPool(appContext)

            try {

              while (rddIter.hasNext) {

                // convert to json
                val hbrow: HBRow = rddIter.next().asInstanceOf[HBRow]

                val jsonCellsMapper = new ObjectMapper()
                jsonCellsMapper.registerModule(DefaultScalaModule)
                val hbrowJson = jsonCellsMapper.writeValueAsString(hbrow)

                // add to eventhub writer batch
                val eventhubWriter = eventhubWriterPool.getOrCreate(hbrow.tableName)
                eventhubWriter.insertLines(hbrowJson)

                // add (topic, docIds) tuple to Set
                tableCount += Tuple2(hbrow.tableName, 1)

              } //end of while

              // flush any eventhub writer's with data
              eventhubWriterPool.flush()

            } finally {
              // close the client connection
              eventhubWriterPool.close()
            }

          } // hasNext

          log.warn(s"SENDING evhub partIndex: {$partIndex} finish numItemsInPartition ${tableCount.size}")

          tableCount.iterator

        })

        // persist
        tableCountList.persist(defaultStorageLevel)

        // count records by topic
        val tableTotalCountRDD: RDD[(String, Int)] = tableCountList.reduceByKey(_ + _)

        //persist
        tableTotalCountRDD.persist(defaultStorageLevel)

        // call an action
        tableTotalCountRDD.foreach(tableCount => {
          val (table, count) = tableCount
          if (count > 0) {
            val tableTotalDuration = System.currentTimeMillis - start
            log.warn(s"""$runtimeApplicationId - ${LogTag.EVENTHUB_PUT_TABLE_COUNT} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
              |table: $table, count: $count,
              |duration: $tableTotalDuration,
              |message: wrote $count records from $table to eventhub in $tableTotalDuration milliseconds"""
              .stripMargin.replaceAll("\n", " "))
          }
        })

        val totalCount = tableTotalCountRDD.values.sum().toLong
        val totalDuration = System.currentTimeMillis - start
        log.warn(s"""$runtimeApplicationId - ${LogTag.PROCESS_RDD_WRITE_EVENTHUB} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTime, batchTimeStr: $batchTimeStr,
          |count: $totalCount,
          |duration: $totalDuration,
          |message: write to  eventhub done in $totalDuration milliseconds"""
          .stripMargin.replaceAll("\n", " "))

        // unpersist
        tableCountList.unpersist()
        tableTotalCountRDD.unpersist()

        totalCount

      } else {
        log.warn(s"$appName skip writing data to eventhub enabled flag is set to $eventhubWriteEnabled...")
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

  /** write contents of {{ RDD[TransformResult] }} to eventhub
    *
    * @return number of records written to eventhub
    */
  def writeTransformResultRDDToEventhub(
    appContext: AppContext,
    batchTime: Long,
    transformResultRDD: RDD[TransformResult]): Long = {

    //create a key value rdditer
    val hbrowRDD: RDD[HBRow] = transformResultRDD.flatMap(transformResult => {
      var hbrowsArray = MutableArrayBuffer.empty[HBRow]
      transformResult.hbaseRows.foreach(rec => {
        val hbrow: HBRow = rec.asInstanceOf[HBRow]
        hbrowsArray += hbrow
      })
      hbrowsArray.toList.iterator
    })
    writeResultsToEventhub(appContext, batchTime, hbrowRDD)
  }
}
