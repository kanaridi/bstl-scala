/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.KafkaConsumer

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import java.util.Properties

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

@SerialVersionUID(100L)
class KafkaTopicConsumer(streamingContext: StreamingContext,
                         directStreamProperties: Map[String, String],
                         val topicSet: Set[String],
                         kafkaTopicLogger : KafkaTopicLoggerTrait) extends Serializable {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  override def toString() = {
    val t = topicSet.toList.mkString(",")
    s"KafkaConsumer[topics:$t]"
  }

  private def createDirectStream(directStreamProperties: Map[String, String],
                                 topicSet: Set[String]): InputDStream[ConsumerRecord[String, String]] = {
    log.info("Entering createDirectStream...")
    log.info(s"topics: $topicSet")
    val ret = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, directStreamProperties))
    log.info("Exiting createDirectStream")

    ret
  }

  def readFromAllTopics(): InputDStream[ConsumerRecord[String, String]] = {
    val topicSetStr = topicSet.toList.mkString(",")
    log.info(s"readFromAlltopics -- topic set = {$topicSetStr}")
    val ts = topicSet.size
    log.info(s"readFromAll - topic set size is $ts")
    log.info("In Constructor, about to create direct stream")
    val directStream = createDirectStream(directStreamProperties, topicSet)
    directStream
  }
}
