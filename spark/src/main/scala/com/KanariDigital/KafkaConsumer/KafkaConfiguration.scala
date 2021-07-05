/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.KafkaConsumer
import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import com.KanariDigital.JsonConfiguration.JsonConfig
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

class KafkaConfiguration extends Serializable {
  val log: Log = LogFactory.getLog("KafkaConfiguration")
  var topics : Option[List[String]] = None
  var batchDuration : Option[Long] = None
  var windowLength: Option[Long] = None
  var slideInterval : Option[Long] = None
  var outputDelimiter : Option[String] = None

  var streamingContextCheckpoint : Option[String] = None

  var brokers : Option[String] = None
  var groupId : Option[String] = None
  var keyDeserializer : Option[String] = None
  var valueDeserializer : Option[String] = None
  var autoOffsetReset : Option[String] = None
  var securityProtocol : Option[String] = None
  var sslTruststoreLocation : Option[String] = None
  var sslKeystoreLocation : Option[String] = None
  var sslKeystorePassword : Option[String] = None
  var sslTruststorePassword : Option[String] = None
  var sslKeyPassword : Option[String] = None
  var zookeeperConnect : Option[String] = None
  var enableAutoCommit : Option[String] = None
  var sessionTimeout : Option[String] = None
  var heartbeatInterval : Option[String] = None


  def validationReasons() : List[String] = {
    var reasons = ArrayBuffer.empty[String]
    if (topics.isDefined == false) reasons += "Topics not defined."
    if (batchDuration.isDefined == false) reasons += "Batch duration not defined."
    if (windowLength.isDefined==false) reasons += "Window length not defined."
    if (slideInterval.isDefined==false) reasons += "Slide interval not defined."
    if (outputDelimiter.isDefined==false) reasons += "Output delimiter is not defined."
    if (securityProtocol.isDefined==false) reasons += "Security protocol is not defined"
    if (enableAutoCommit.isDefined==false) reasons += "Enable auto commit is not defined"
    if (enableAutoCommit.get.toString != "false") reasons += "Enable auto commit must be set to false"
    if (sessionTimeout.isDefined==false) reasons += "Session timeout is not defined"
    if (heartbeatInterval.isDefined==false) reasons += "Heartbeat interval is not defined"
    if (securityProtocol.isDefined && securityProtocol.get == "SSL") {
      if(sslTruststoreLocation.isDefined == false) reasons += "SSL Truststore location not defined"
      if(sslKeystoreLocation.isDefined==false) reasons += "SSL Keystore location not defined"
      if(sslKeystorePassword.isDefined==false) reasons += "SSL Keystore password not defined"
      if(sslTruststorePassword.isDefined==false) reasons += "SSL TrustStore password not defined"
      if(sslKeyPassword.isDefined==false) reasons += "SSL Key Password not defined"
    }
    return reasons.toList
  }

  def isValid() : Boolean = validationReasons().size == 0

  override def toString() : String = {
    val undef = "Undefined"
    val topicStr = if(topics.isDefined) topics.get.mkString(",") else undef
    val bdStr = if (batchDuration.isDefined) batchDuration.get.toString() else undef
    val wlStr = if (windowLength.isDefined) windowLength.get.toString else undef
    val siStr = if (slideInterval.isDefined) slideInterval.get.toString else undef
    val outDelStr = outputDelimiter.getOrElse(undef)
    val scxtcheckpoint = streamingContextCheckpoint.getOrElse(undef)
    val brokersStr = brokers.getOrElse(undef)
    val groupIdStr = groupId.getOrElse(undef)
    val keyDesStr = keyDeserializer.getOrElse(undef)
    val valDesStr = valueDeserializer.getOrElse(undef)
    val autoOffsetStr = autoOffsetReset.getOrElse(undef)
    val secProtStr = securityProtocol.getOrElse(undef)
    val tslStr = sslTruststoreLocation.getOrElse(undef)
    val kslStr = sslKeystoreLocation.getOrElse(undef)
    val kspassStr = sslKeystorePassword.getOrElse(undef)
    val tspStr = sslTruststorePassword.getOrElse(undef)
    val sslKeyPassStr = sslKeyPassword.getOrElse(undef)
    val zookeeperConnectStr = zookeeperConnect.getOrElse(undef)
    val enableAutoCommitStr = enableAutoCommit.getOrElse(undef)
    val sessionTimeoutStr = sessionTimeout.getOrElse(undef)
    val heartbeatIntervalStr = heartbeatInterval.getOrElse(undef)
    s"""
       | KafkaConfiguration[ topics:                     '$topicStr'
       |                     batchDuration:              $bdStr
       |                     windowLength:               $wlStr
       |                     slideInterval:              $siStr
       |                     outputDelimiter:            '$outDelStr
       |                     streamingContextCheckpoint: $scxtcheckpoint
       |                     brokers:                    $brokersStr
       |                     group.id:                   $groupIdStr
       |                     key.deserializer:           $keyDesStr
       |                     value.deserializer:         $valDesStr
       |                     auto.offset.reset           $autoOffsetStr
       |                     security.protocol           $secProtStr
       |                     ssl.truststore.location     $tslStr
       |                     ssl.keystore.location       $kslStr
       |                     ssl.keystore.password       $kspassStr
       |                     ssl.truststore.password     $tspStr
       |                     ssl.key.password            $sslKeyPassStr
       |                     zookeeper.connect           $zookeeperConnectStr
       |                     enable.auto.commit          $enableAutoCommitStr
       |                     session.timeout             $sessionTimeoutStr
       |                     heartbeat.interval          $heartbeatIntervalStr
       | ]
     """.stripMargin
  }
}

object KafkaConfiguration {
  def readJsonFile(inputStream: InputStream): Try[KafkaConfiguration] = {
    val jsonConfig = new JsonConfig
    jsonConfig.read(inputStream, Some("kafka"))

    var kafkaConfiguration = new KafkaConfiguration

    val topicArray = jsonConfig.getJsonArray("topics")

    if (topicArray.isDefined) {
      val n: Int = topicArray.get.size
      val theTopics: List[String] = (0 to (n-1)).map(i => topicArray.get.get(i).asInstanceOf[String]).toList
      kafkaConfiguration.topics = Some(theTopics)
    } else {
      val singleTopic = jsonConfig.getStringValue("topics")
      kafkaConfiguration.topics = Some(List(singleTopic.get))
    }

    kafkaConfiguration.batchDuration = jsonConfig.getLongValue("batchDuration")
    kafkaConfiguration.windowLength = jsonConfig.getLongValue("windowLength")
    kafkaConfiguration.slideInterval = jsonConfig.getLongValue("slideInterval")
    kafkaConfiguration.outputDelimiter = jsonConfig.getStringValue("outputDelimiter")

    kafkaConfiguration.streamingContextCheckpoint = jsonConfig.getStringValue("checkpoint")

    kafkaConfiguration.brokers = jsonConfig.getStringValue("brokers")
    kafkaConfiguration.groupId = jsonConfig.getStringValue("group.id")
    kafkaConfiguration.keyDeserializer = jsonConfig.getStringValue("key.deserializer")
    kafkaConfiguration.valueDeserializer = jsonConfig.getStringValue("value.deserializer")
    kafkaConfiguration.autoOffsetReset = jsonConfig.getStringValue("auto.offset.reset")
    kafkaConfiguration.securityProtocol = jsonConfig.getStringValue("security.protocol")
    kafkaConfiguration.sslTruststoreLocation = jsonConfig.getStringValue("ssl.truststore.location")
    kafkaConfiguration.sslKeystoreLocation = jsonConfig.getStringValue("ssl.keystore.location")
    kafkaConfiguration.sslKeystorePassword = jsonConfig.getStringValue("ssl.keystore.password")
    kafkaConfiguration.sslTruststorePassword = jsonConfig.getStringValue("ssl.truststore.password")
    kafkaConfiguration.sslKeyPassword = jsonConfig.getStringValue("ssl.key.password")
    kafkaConfiguration.zookeeperConnect = jsonConfig.getStringValue("zookeeper.connect")
    kafkaConfiguration.enableAutoCommit = jsonConfig.getStringValue("enable.auto.commit")
    kafkaConfiguration.sessionTimeout = jsonConfig.getStringValue("session.timeout")
    kafkaConfiguration.heartbeatInterval = jsonConfig.getStringValue("heartbeat.interval")


    Success(kafkaConfiguration)
  }

  def readJsonFile(path : String, config : Configuration ) : Try[KafkaConfiguration] = {
    val fileSystem = FileSystem.get(config)
    val inputStream = fileSystem.open(new Path(path))
    val kafkaConfig = readJsonFile(inputStream)
    inputStream.close()
    kafkaConfig
  }
}
