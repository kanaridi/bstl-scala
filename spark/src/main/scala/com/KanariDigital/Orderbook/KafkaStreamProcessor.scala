/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook

import com.KanariDigital.KafkaConsumer.KafkaTopicConsumer
import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, HeartBeatStatusEmitter, UnitOfWork}
import com.KanariDigital.Orderbook.HdfsHiveWriter.OrderBookContext
import com.KanariDigital.app.RddProcessor
import com.KanariDigital.app.util.SparkUtils

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}

import scala.collection.mutable.ArrayBuffer

abstract trait KafkaStreamProcessorTrait {
  def run(kafkaTopicConsumer: KafkaTopicConsumer): Unit
}

class KafkaStreamProcessor(appName: String, orderBookContext : OrderBookContext) extends Serializable with KafkaStreamProcessorTrait {

  val log: Log = LogFactory.getLog(this.getClass.getName)
  private var heartBeat : Option[HeartBeatStatusEmitter] = None

  val rddProcessor = new RddProcessor(appName, orderBookContext)

  def setHeartbeat(heartBeat : HeartBeatStatusEmitter) : Unit = {
    this.heartBeat = Some(heartBeat)
  }

  def writeAllTopicsToHdfsHive(topicSet : Set[String], rdd: RDD[(String, String)]) : Option[Exception] = {

    try {
      val hdfsHiveWriter = orderBookContext.hdfsHiveFactory.createHdfsHiveWriter()

      if (hdfsHiveWriter.isDefined) {
        val start = System.currentTimeMillis
        log.info("Writing Raw JSON to HDFS")
        val dataTopics = SparkUtils.getDistinctTopicsDF(rdd)
        val distinctDuration = System.currentTimeMillis - start
        log.info(s"writeAllTopicsToHdfsHive: found ${dataTopics.size} topics: ${dataTopics.mkString(",")} in ${distinctDuration} milliseconds")
        log.info(s"writeAllTopicsToHdfsHive: writing to HDFS: ${dataTopics.size} topics: ${dataTopics.mkString(",")}: start")
        dataTopics.foreach(topic =>
          hdfsHiveWriter.get.write(topic, rdd.filter(pr => pr._1 == topic).map(pr => pr._2)))
        val duration = System.currentTimeMillis - start
        log.info(s"writeAllTopicsToHdfsHive: writing to HDFS: ${dataTopics.size} topics: ${dataTopics.mkString(",")}: done. in $duration milliseconds")
      } else {
        log.info("Skipping Raw JSON writing.")
      }

    } catch {
      case ex: Exception => {
        val mesg = ex.getMessage
        log.error(s"WriteTopicTo HdfsHive, Exception: $mesg")
        return Some(ex)
      }
    }
    None
  }

  override def run(kafkaTopicConsumer: KafkaTopicConsumer): Unit = {

    log.info("Reading all topics.")

    val cStr = kafkaTopicConsumer.toString()
    log.info(s"ka: $cStr")

    // use Spark-Kafka_0.10 method (which uses new kafka consumer api) ,
    // and get InputDStream[ConsumerRecord(String,String)]
    val messages = kafkaTopicConsumer.readFromAllTopics()
    log.info("Completed read from all topics")

    val topicSet = kafkaTopicConsumer.topicSet
    var offsetArray = ArrayBuffer.empty[OffsetRange]
    var uow : UnitOfWork = null


    // obtain offset information for each partition that was read from kafka
    // and write to hdfs audit log
    val messagesWithOffsetRanges = messages.transform { rdd =>
      try {

        // first method invoked on InputDStream, in order to get offsets
        val r = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetArray ++= r

        uow = UnitOfWork.createUnitOfWork()

      } catch {
        case _: Exception => {
          log.info("Not adding offset range... skipping")
        }
      }
      rdd
    }

    val auditLog = CompositeAuditLogger.createHdfsAndLogAuditLogger(orderBookContext.hdfsAuditLoggerConfig)

    // extract ConsumerRecord(String, String) key and value pairs
    // value is json format
    val topicAndJson = messagesWithOffsetRanges.map(cr => (cr.topic(), cr.value()))
    log.info("Run: Performing map")

    var encounteredException : Option[Exception] = None
    topicAndJson.foreachRDD(rdd => {

      if (heartBeat.isDefined) {
        val hb = heartBeat.get.signalAlive()
      }

      // cache rdd, to prevent fetching data from kafka
      // again and again. This should cache the rdd after
      // fetching data from kafka for the first time.
      rdd.persist()

      // check of consumer record rdd has any elements
      if (rdd.isEmpty()) {

        log.info("Nothing to process ...")

      } else {

        // do the data transformation
        encounteredException = this.rddProcessor.processRDD(uow, rdd, auditLog)
        if (encounteredException.isDefined == false) {
          //write the raw records to hdfs
          encounteredException = writeAllTopicsToHdfsHive(topicSet, rdd)
        }


        if (encounteredException.isDefined) {
          orderBookContext.errorLogger.logException(encounteredException.get)
        } else {
          try {

            log.info("About to commit offsets...")
            messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetArray.toArray)
            log.info("Finished committing offsets.")

            log.info("About to log offsets to the audit logs...")
            //going to log the offsets
            val auditLogger = CompositeAuditLogger.createHdfsAndLogAuditLogger(orderBookContext.hdfsAuditLoggerConfig)
            auditLogger.logKafkaOffsetRanges(uow, offsetArray)
            auditLogger.flush()
            log.info("Done logging offsets to the audit logs.")

            //reset offsetArray
            offsetArray = ArrayBuffer.empty[OffsetRange]


          } catch {
            case ex : Exception => {
              log.error(s"Unable to commit offsets: ${ex.toString()}")
              orderBookContext.errorLogger.logException(ex)
            }
          }
        }

      }// else isEmpty

      // done processing data, now remove rdd from cache
      // so we dont run out memory
      rdd.unpersist()

     log.info("Completed processKafkaConsumer")

    })
  }
}
