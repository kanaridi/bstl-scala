/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.KafkaConsumer

import java.util.Properties

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import scala.util.{Success, Try}


class KafkaConsumerFactory(kafkaConfiguration: KafkaConfiguration,
                           sparkStreamingContext : StreamingContext) {
  val log: Log = LogFactory.getLog("KafkaConsumerFactory")
  var offsets = scala.collection.mutable.Map[TopicPartition, Long]()


  def getTopics() : Set[String] = {
    if (kafkaConfiguration.topics.isDefined == false) {
      val mesg = "Fatal: No Kafka topics supplied"
      log.error(mesg)
      throw new RuntimeException(mesg)
    }
    kafkaConfiguration.topics.get.toSet
  }

  val params  = List(
    ("bootstrap.servers", kafkaConfiguration.brokers),
    ("key.deserializer", kafkaConfiguration.keyDeserializer),
    ("value.deserializer", kafkaConfiguration.valueDeserializer),
    ("group.id", kafkaConfiguration.groupId),
    ("auto.offset.reset", kafkaConfiguration.autoOffsetReset),
    ("security.protocol", kafkaConfiguration.securityProtocol),
    ("ssl.truststore.location", kafkaConfiguration.sslTruststoreLocation),
    ("ssl.truststore.password", kafkaConfiguration.sslTruststorePassword),
    ("ssl.keystore.location", kafkaConfiguration.sslKeystoreLocation),
    ("ssl.keystore.password", kafkaConfiguration.sslKeystorePassword),
    ("ssl.key.password", kafkaConfiguration.sslKeyPassword),
    ("zookeeper.connect", kafkaConfiguration.zookeeperConnect),
    ("enable.auto.commit", kafkaConfiguration.enableAutoCommit),
    ("session.timeout.ms", kafkaConfiguration.sessionTimeout),
    ("heartbeat.interval.ms", kafkaConfiguration.heartbeatInterval)
    )

  /** Parse JSON property file and create a Map object for kafka
    direct stream */
  def constructDirectStreamProperties() : Map[String, String] = {
    params.filter(x => x._2.isDefined)
      .map(x => x._1 -> x._2.get)
      .toMap
  }

  private def create(
    streamingContext: StreamingContext,
    directStreamProperties: Map[String,String],
    topics : Set[String],
    kafkaTopicLogger : KafkaTopicLoggerTrait) : KafkaTopicConsumer = {

    log.info("Am in KafkaConsumerFactory create")
    return new KafkaTopicConsumer(streamingContext, directStreamProperties, topics, kafkaTopicLogger)

  }

}

object KafkaConsumerFactory {
  def apply(
    kafkaConfiguration: KafkaConfiguration,
    sparkStreamingContext : StreamingContext,
    kafkaTopicLogger : KafkaTopicLoggerTrait): Try[KafkaTopicConsumer] = {

    val factory = new KafkaConsumerFactory(kafkaConfiguration,sparkStreamingContext)
    val topics = factory.getTopics()
    val dStreamProperties = factory.constructDirectStreamProperties()

    val consumer = factory.create(sparkStreamingContext, dStreamProperties, topics, kafkaTopicLogger )
    Success(consumer)

  }
}
