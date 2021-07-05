/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app

import java.io.{InputStream, InputStreamReader, Serializable}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import scala.util.{Success, Try, Failure}

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.commons.logging.{Log, LogFactory}

import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}

import com.KanariDigital.JsonConfiguration.JsonConfig
import com.kanaridi.common.util.HdfsUtils


final case class PipelineConfigException(
  private val message:String = "",
  private val cause: Throwable = None.orNull) extends Exception(message, cause)


class Source(jsonObj: JSONObject) extends Serializable {

  val connectionId: Option[String] = Option(jsonObj.get("connectionId").asInstanceOf[String])
  val connectionType: Option[String] = Option(jsonObj.get("connectionType").asInstanceOf[String])
  val options: Option[JSONObject] = Option(jsonObj.get("options").asInstanceOf[JSONObject])

  override def toString(): String = {

    val connectionIdStr = if (connectionId.isDefined) connectionId.get.toString else ""
    val connectionTypeStr = if (connectionType.isDefined) connectionType.get.toString else ""
    val optionsJSONStr = if (options.isDefined) options.get.toString else ""

    s"""
    source:
       connectionId:
           $connectionIdStr
       connectionType:
           $connectionTypeStr
       options:
           $optionsJSONStr""".stripMargin
  }
}

class Sink(jsonObj: JSONObject) extends Serializable {

  val connectionId: Option[String] = Option(jsonObj.get("connectionId").asInstanceOf[String])
  val connectionType: Option[String] = Option(jsonObj.get("connectionType").asInstanceOf[String])
  val options: Option[JSONObject] = Option(jsonObj.get("options").asInstanceOf[JSONObject])

  override def toString(): String = {

    val connectionIdStr = if (connectionId.isDefined) connectionId.get.toString else ""
    val connectionTypeStr = if (connectionType.isDefined) connectionType.get.toString else ""
    val optionsJSONStr = if (options.isDefined) options.get.toString else ""

    s"""
    sink:
        connectionId:
            $connectionIdStr
        connectionType:
            $connectionTypeStr
        options:
            $optionsJSONStr""".stripMargin
  }
}

class Signals(jsonObj: JSONObject) extends Serializable {

  val log: Log = LogFactory.getLog("Connection")

  val sourcesJsonArray:Option[JSONArray] = Option(jsonObj.get("sources").asInstanceOf[JSONArray])

  var sourcesBuf: ListBuffer[Source] = new ListBuffer[Source]()
  if (sourcesJsonArray.isDefined) {
    val iter = sourcesJsonArray.get.iterator
    while (iter.hasNext) {
      val subObj = iter.next
      sourcesBuf += new Source(subObj.asInstanceOf[JSONObject])
    }
  }
  val sources: List[Source] = sourcesBuf.toList

  val sinksJsonArray:Option[JSONArray] = Option(jsonObj.get("sinks").asInstanceOf[JSONArray])
  var sinksBuf: ListBuffer[Sink] = new ListBuffer[Sink]()
  if (sinksJsonArray.isDefined) {
    val iterSink = sinksJsonArray.get.iterator
    while (iterSink.hasNext) {
      val subObj = iterSink.next
      sinksBuf += new Sink(subObj.asInstanceOf[JSONObject])
    }
  }
  val sinks: List[Sink] = sinksBuf.toList

  override def toString(): String = {

    val sourcesStr = sources.mkString("\n")
    val sinksStr = sinks.mkString("\n")

    s"""
    sources:
       $sourcesStr
    sinks:
       $sinksStr""".stripMargin
  }
}

class Operation(jsonObj: JSONObject) extends Serializable {

  val operatorId: Option[String] = Option(jsonObj.get("operatorId").asInstanceOf[String])
  val operatorType: Option[String] = Option(jsonObj.get("operatorType").asInstanceOf[String])
  val operationName: Option[String] = Option(jsonObj.get("name").asInstanceOf[String])
  val operationDescription: Option[String] = Option(jsonObj.get("description").asInstanceOf[String])
  val signalsObj: Option[JSONObject] = Option(jsonObj.get("signals").asInstanceOf[JSONObject])
  val signals: Option[Signals] = Option(new Signals(signalsObj.get))
  val mapperPath: Option[String] = Option(if (jsonObj.get("mapper").isInstanceOf[String]) jsonObj.get("mapper").asInstanceOf[String] else "")
  val mapperObj: Option[JSONObject] = Option(if (jsonObj.get("mapper").isInstanceOf[JSONObject]) jsonObj.get("mapper").asInstanceOf[JSONObject] else new JSONObject())
  val options: Option[JSONObject] = Option(jsonObj.get("options").asInstanceOf[JSONObject])
  val filesArray:Option[JSONArray] = Option(jsonObj.get("files").asInstanceOf[JSONArray])
  var filesBuf: ListBuffer[String] = new ListBuffer[String]()
  if (filesArray.isDefined && filesArray.get != null) {
    val iterSink = filesArray.get.iterator
    while (iterSink.hasNext) {
      val subObj = iterSink.next
      filesBuf += subObj.asInstanceOf[String]
    }
  }
  val files: List[String] = filesBuf.toList

  override def toString(): String = {

    val operatorIdStr = if (operatorId.isDefined) operatorId.get.toString else ""
    val operationNameStr = if (operationName.isDefined) operationName.get.toString else ""
    val operationDescriptionStr = if (operationDescription.isDefined) operationDescription.get.toString else ""
    val mapperPathStr = if (mapperPath.isDefined) mapperPath.get.toString else ""
    val mapperObjStr = if (mapperObj.isDefined) mapperObj.get.toString else ""
    val signalsStr = if (signals.isDefined) signals.get.toString else ""
    val optionsStr = if (options.isDefined) options.get.toString else ""

    s"""
    operation:
      operatorId:
        $operatorIdStr
      name:
        $operationNameStr
      description:
        $operationDescriptionStr
      mapperPath:
        $mapperPathStr
      mapperObj:
        $mapperObjStr
      signals:
        $signalsStr
      options:
        $optionsStr""".stripMargin
  }
}

class Connection(jsonObj: JSONObject) extends Serializable {

  val log: Log = LogFactory.getLog("Connection")

  val id: Option[String] = Option(jsonObj.get("id").asInstanceOf[String])
  val connectionType: Option[String] = Option(jsonObj.get("connectionType").asInstanceOf[String])
  val name: Option[String] = Option(jsonObj.get("name").asInstanceOf[String])
  val connection: Option[JSONObject] = Option(jsonObj.get("connection").asInstanceOf[JSONObject])

  val topicsJsonArray = Option(jsonObj.get("topics").asInstanceOf[JSONArray])
  var topicsBuf: ListBuffer[String] = new ListBuffer[String]()
  if (topicsJsonArray.isDefined && topicsJsonArray.get != null) {
    // log.info("topicsJsonArray: " + topicsJsonArray.toString)
    val iterSink = topicsJsonArray.get.iterator
    while (iterSink.hasNext) {
      val subObj = iterSink.next
      topicsBuf += subObj.asInstanceOf[String]
    }
  }
  val topics: List[String] = topicsBuf.toList
  val options: Option[JSONObject] = Option(jsonObj.get("options").asInstanceOf[JSONObject])

  override def toString(): String = {

    val idStr = if (id.isDefined) id.get else ""
    val connectionTypeStr = if (connectionType.isDefined) connectionType.get else ""
    val nameStr = if (name.isDefined) name.get else ""
    val connectionStr = if (connection.isDefined) connection.get else ""
    val topicsStr = topics.mkString("\n")
    val optionsStr = if (options.isDefined) options.get.toString else ""

    s"""
    connection:
        id:
            $idStr
        connectionType:
            $connectionTypeStr
        name:
            $nameStr
        topics:
            $topicsStr
        connection:
            $connectionStr
        options:
            $optionsStr
     """.stripMargin
  }
}

/** Parse pipeline config into a object */
class PipelineConfig(jsonObj: JSONObject) extends Serializable {

  val log: Log = LogFactory.getLog("PipelineConfig")

  //id
  val id: Option[String] = Option(jsonObj.get("id").asInstanceOf[String])

  //name
  val name: Option[String] = Option(jsonObj.get("name").asInstanceOf[String])

  //description
  val description: Option[String] = Option(jsonObj.get("description").asInstanceOf[String])

  //operations
  val operationsJsonArray: Option[JSONArray] = Option(jsonObj.get("operations").asInstanceOf[JSONArray])
  var operationsBuf: ListBuffer[Operation] = new ListBuffer[Operation]()
  if (operationsJsonArray.isDefined) {
    val iter = operationsJsonArray.get.iterator
    while (iter.hasNext) {
      val subObj = iter.next
      // log.info("subObj: " + subObj.toString)
      operationsBuf += new Operation(subObj.asInstanceOf[JSONObject])
    }
  }
  val operations: List[Operation] = operationsBuf.toList

  //connections
  val connectionsJsonArray: Option[JSONArray] = Option(jsonObj.get("connections").asInstanceOf[JSONArray])
  var connectionsBuf: ListBuffer[Connection] = new ListBuffer[Connection]()
  if (connectionsJsonArray.isDefined) {
    val iterConnSource = connectionsJsonArray.get.iterator
    while (iterConnSource.hasNext) {
      val subObj = iterConnSource.next
      // log.info("subObj: " + subObj.toString)
      connectionsBuf += new Connection(subObj.asInstanceOf[JSONObject])
    }
  }
  val connections: List[Connection] = connectionsBuf.toList
  val connectionsMap: Map[String, Connection] = connections.map(conn => (conn.id.get, conn)).toMap

  // options
  val options: Option[JSONObject] = Option(jsonObj.get("options").asInstanceOf[JSONObject])

  def isValid(): Boolean = {

    if (!id.isDefined) {
      return false
    }

    if (id.getOrElse("") == "") {
      return false
    }

    return true
  }


  /** convert [[PipelineConfig]] object to string for debugging/logging purposes
    *
    *  @returns string representation of [[PipelineConfig]] object
    */
  override def toString() : String = {

    val idStr =  if (id.isDefined) id.get.toString else ""
    val nameStr =  if (name.isDefined) name.get.toString else ""
    val descriptionStr = if (description.isDefined) description.get.toString else ""
    val operationsStr = operations.mkString("\n")
    val connectionsStr = connections.mkString("\n")
    val optionsStr = if (options.isDefined) options.get.toString else ""

    s"""
    pipeline:
        id: $idStr
        name: $nameStr
        description: $descriptionStr
        operations:
            $operationsStr
        connections:
            $connectionsStr
        options:
            $optionsStr""".stripMargin
  }
}

object PipelineConfig {

  val log: Log = LogFactory.getLog("PipelineConfigObject")

  /** read json config file for specified appName
    *
    *  @param inputStream json config file input stream
    *  @return [[PipelineConfig]] object
    */
  def readJsonFile(inputStream: InputStream): PipelineConfig = {
    val jsonIn = new JSONParser().parse(new InputStreamReader(inputStream))
    val jsonObj = jsonIn.asInstanceOf[JSONObject]
    val pipelineConfig = new PipelineConfig(jsonObj)
    // is pipelineConfig valid?
    if (pipelineConfig.isValid()) {
      pipelineConfig
    } else {
      throw new Exception("PipelineConfig is not valid")
    }
  }

  def readJsonFile(path : String, config: HadoopConfiguration) : PipelineConfig = {
    val fileSystem = FileSystem.get(config)
    val inputStream = fileSystem.open(new Path(path))
    val pipelineConfig = readJsonFile(inputStream)
    inputStream.close()
    pipelineConfig
  }

  /** converts Map[String, String]] object to a string
    * {{
    * key1 => value1, key2 => value2
    * }}
    */
  def mapToString(myMap: Map[String, String]): String = {
      val mapStr = myMap.map { case(key, value) => s"$key=$value" }.mkString(", ")
      mapStr
  }

  def checkEmptyValue(valueFunc:() => String, errorMessage: String): String = {
    val valResult = valueFunc()
    if (valResult  != "") valResult else throw new PipelineConfigException(errorMessage)
  }

  def checkEmptyValue(value: String, errorMessage: String): String = {
    if (value  != "") value else throw new PipelineConfigException(errorMessage)
  }

  /** parse "mapping"  options
    * parses options that can be in two formats
    *  Format #1:
    *  {{
    *   { "$topicName": "$hdfsDir"}
    *   e.g.
    *   {"shipmentDetails": "shipmentdetails"}
    *  }}
    *  OR
    *  Format #2:
    * {{
    *  {"key": "$topicName", "value": "$hdfsDirName"}
    *  e.g.
    *  {"key": "shipmentDetails", "value": "shipmentdetails"}
    * }}
    */
  def parseStorageOptionsMappingList(storageMapConnection: Map[String, Any]): Map[String, Any] = {

    val mappingList: Some[Any] = storageMapConnection.get("mapping").asInstanceOf[Some[Any]]
    var storageMap: Map[String, Any] = Map[String, Any]()
    if (mappingList.isDefined) {
      val iter  = mappingList.get.asInstanceOf[List[Any]].iterator
      while (iter.hasNext) {
        val subObj: Map[String, Any] = iter.next.asInstanceOf[Map[String, Any]]

        if (subObj.contains("key")) {
          // special map, format#2
          val topicNameOpt = subObj.get("key")
          if (topicNameOpt.isDefined) {
            val topicName = topicNameOpt.get.asInstanceOf[String]
            val hdfsDirName = subObj.getOrElse("value", topicName).toString
            storageMap += topicName -> hdfsDirName
          }
        } else {
          // special map, format#1
          for ((storageTopic, storageMapDir) <- subObj) {
            val topicName = storageTopic.asInstanceOf[String]
            val hdfsDirName = storageMapDir.toString
            storageMap += topicName-> hdfsDirName
          }
        }
      }
    } // mappingList

    storageMap
  }

  /** convert a pipeline config to app config that can be consumed by 000010
    *  streaming apps */
  def convertToAppConfig(pipelineConfig: PipelineConfig): AppConfig = {

    var appConfig = new AppConfig

    // id
    val pipelineId = checkEmptyValue(
      valueFunc = () => { if(pipelineConfig.id.isDefined) pipelineConfig.id.get else "" },
      errorMessage="pipeline id is not defined")

    // name
    val pipelineName = checkEmptyValue(
      valueFunc = () => { if (pipelineConfig.name.isDefined) pipelineConfig.name.get else "" },
      errorMessage = "pipeline name is not defined")

    // operation
    val firstOperation: Operation = if (pipelineConfig.operations.size > 0) pipelineConfig.operations.head else throw new PipelineConfigException("operations are not defined") //first operation

    // signals
    val signals: Signals = if (firstOperation.signals.isDefined) firstOperation.signals.get else throw new PipelineConfigException("signals are not defined")

    // first source
    val firstSource: Source = if (signals.sources.size > 0) signals.sources.head  else throw new PipelineConfigException("sources are not defined") //first source

    // sinks
    val sinks: List[Sink] = if (signals.sinks.size > 0) signals.sinks else throw new PipelineConfigException("sinks are not defined")

    // connections map
    val connectionIdConnectionMap: Map[String, Connection] = pipelineConfig.connectionsMap

    // pipeline options
    val pipelineOptionsMap = toMap(pipelineConfig.options)

    //set app id
    appConfig.appId = pipelineId

    //set appname
    appConfig.appName = pipelineName

    //set appConfigMap
    var appMap = Map[String, String]()
    var sparkMap = Map[String, String]()
    var messageMap = Map[String, String]()

    // rules
    var ruleLogConfigMap = Map[String, String]()
    var uberRulesConfigMap = Map[String, Map[String, String]]()
    var rulesConfigMap = Map[String, String]()
    var ruleTestConfigMap = Map[String, String]()

    // sources
    var kafkaSourceMap = Map[String, String]()
    var eventhubSourceMap = Map[String, String]()
    var hdfsSourceMap = Map[String, String]()
    var abfsSourceMap = Map[String, String]()

    // sinks
    var storeRawMap = Map[String, String]()
    var storeRawMappingMap = Map[String, String]()
    var hbaseSinkMap = Map[String, String]()
    var eventhubSinkMap = Map[String, String]()

    //audit log
    var auditLogMap = Map[String, String]()


    // set mapper
    val mPath = firstOperation.mapperPath
    // mapping file location
    if (mPath.isDefined && mPath.get != "") {
      messageMap += "path" -> mPath.get.toString
    } else if (firstOperation.mapperObj.isDefined) {
      val mObj = firstOperation.mapperObj
      messageMap += "mapperObj" -> mObj.get.toString
    }

    val operationOptionsMap = toMap(firstOperation.options)
    if (operationOptionsMap.isDefined) {
      val operationOptions = operationOptionsMap.get
      val enableDedup = operationOptions.getOrElse("enableDedup", "").toString
      val dedupColfamily = operationOptions.getOrElse("dedupColfamily", "").toString
      val dedupColname = operationOptions.getOrElse("dedupColname", "").toString
      val dedupLookback = operationOptions.getOrElse("dedupLookback", "").toString
      val maxBatchSize = operationOptions.getOrElse("maxBatchSize", "").toString
      val batchDuration = operationOptions.getOrElse("batchDuration", "").toString
      val checkpoint = operationOptions.getOrElse("checkpoint", "").toString

      val defaultStorageLevel = operationOptions.getOrElse(
        "defaultStorageLevel",
        "MEMORY_AND_DISK_SER").toString

      if (enableDedup != "") {
        appMap += "enable-dedup" -> enableDedup
        if (enableDedup == "true") {
          appMap += "dedup-colfamily" -> checkEmptyValue(value=dedupColfamily, errorMessage="dedupColfamily is empty")
          appMap += "dedup-colname" -> checkEmptyValue(value=dedupColname, errorMessage="dedupColname is empty")
          appMap += "dedup-lookback" -> checkEmptyValue(value=dedupLookback, errorMessage="dedupLookback is empty")
        }
      }

      appMap += "max-batch-size" -> maxBatchSize
      appMap += "default-storage-level" -> defaultStorageLevel

      // spark options
      if (batchDuration != "") {
        sparkMap += "batchDuration" -> batchDuration
      }

      sparkMap += "checkpoint" -> checkpoint

      //audit log options
      if (pipelineOptionsMap.isDefined) {

        val pipelineOptions = pipelineOptionsMap.get.asInstanceOf[Map[String,Any]]
        val logPath = pipelineOptions.getOrElse("logPath", "").toString

        // get app log dir, if path is relative construct path in users
        // home directory
        val appLogPath = HdfsUtils.getAppDir(logPath, pipelineId)
        if (appLogPath.isDefined) {
          val dirName = appLogPath.get.toUri.getPath
          log.info(s"creating LOG directory: $dirName")
          HdfsUtils.createDirectory(dirName)
        }

        //FIXME: rename logSuffix to logPrefix in schema
        val logPrefix = pipelineOptions.getOrElse("logSuffix", "").toString

        auditLogMap += "path" -> appLogPath.get.toUri.getPath
        auditLogMap += "filename-prefix" -> logPrefix
      }

    }

    // source
    val sourceConnectionId = firstSource.connectionId.get
    firstSource.connectionType.get match {
      case "kafka" => {

        //set appConfig appType
        appConfig.appType = AppType.KAFKA_STREAMING_APP_TYPE

        //options
        // get connection for this source
        val conn = connectionIdConnectionMap(sourceConnectionId)
        val sourceConnectionMap = toMap(conn.connection).get.asInstanceOf[Map[String,Any]]

        // connectionStrings
        val connectionStringsMap = sourceConnectionMap("connectionStrings").asInstanceOf[Map[String,Any]]
        val url = connectionStringsMap.getOrElse("url", "").toString

        // security
        val securityMap = sourceConnectionMap("security").asInstanceOf[Map[String,Any]]
        val hostType = securityMap.getOrElse("hostType", "").toString
        val port = securityMap.getOrElse("port", "").toString
        val protocol = securityMap.getOrElse("protocol", "").toString
        val truststoreLocation = securityMap.getOrElse("truststoreLocation", "").toString
        val truststorePassword = securityMap.getOrElse("truststorePassword", "").toString
        val keystoreLocation = securityMap.getOrElse("keystoreLocation", "").toString
        val keystorePassword = securityMap.getOrElse("keystorePassword", "").toString
        val keyPassword = securityMap.getOrElse("keyPassword", "").toString

        // get connection options
        val sourceOptionsMap = toMap(firstSource.options).get
        val connOptionsMap = toMap(conn.options).get

        // consumerGroup
        val consumerGroupConn = connOptionsMap.getOrElse("consumerGroup", "").toString
        val consumerGroup: String = sourceOptionsMap.getOrElse("consumerGroup", "").toString

        val autoOffsetResetConn = connOptionsMap.getOrElse("autoOffsetReset", "").toString
        val autoOffsetReset = sourceOptionsMap.getOrElse("autoOffsetReset", "").toString

        val enableAutoCommitConn = connOptionsMap.getOrElse("enableAutoCommit", "").toString
        val enableAutoCommit = sourceOptionsMap.getOrElse("enableAutoCommit", "").toString

        // deserializer
        val deserializerMapConn = connOptionsMap.getOrElse("deserializer", Map[String, Any]()).asInstanceOf[Map[String,Any]]
        val deserializerMapSource = sourceOptionsMap.getOrElse("deserializer", Map[String, Any]()).asInstanceOf[Map[String,Any]]

        val deserializerKeyConn = deserializerMapConn.getOrElse("key", "").toString
        val deserializerKey = deserializerMapSource.getOrElse("key", "").toString

        val deserializerValueConn = deserializerMapConn.getOrElse("value", "").toString
        val deserializerValue = deserializerMapSource.getOrElse("value", "").toString

        //topics
        val topicList = conn.topics
        kafkaSourceMap += "topics" -> checkEmptyValue(
          valueFunc = () => { if (topicList.size > 0) topicList.mkString(",") else  "" },
          errorMessage="kafka: topics are not defined")

        //set kafka config
        kafkaSourceMap += "group.id" -> checkEmptyValue(
          valueFunc = () => { if (consumerGroup != "") consumerGroup else consumerGroupConn },
          errorMessage="kafka: consumerGroup is not defined")

        kafkaSourceMap += "key.deserializer" -> checkEmptyValue(
          valueFunc = () => { if (deserializerKey != "") deserializerKey else deserializerKeyConn },
          errorMessage="kafka: deserializerKey is not defined")

        kafkaSourceMap += "value.deserializer" -> checkEmptyValue(
          valueFunc = () => {if (deserializerValue != "") deserializerValue else deserializerValueConn},
          errorMessage="kafka: deserializerValue is not defined")

        kafkaSourceMap += "brokers" -> checkEmptyValue(
          value = url,
          errorMessage="kafka: url for brokers is not defined")

        kafkaSourceMap += "auto.offset.reset" -> checkEmptyValue(
          valueFunc = () => { if (autoOffsetReset !="") autoOffsetReset else autoOffsetResetConn },
          errorMessage="kafka: auto offset reset is not defined")

        kafkaSourceMap += "enable.auto.commit" -> checkEmptyValue(
          valueFunc = () => { if (enableAutoCommit !="") enableAutoCommit else enableAutoCommitConn },
          errorMessage="kafka: enable auto commit is not defined")

        kafkaSourceMap += "security.protocol" -> checkEmptyValue(
          value = protocol,
          errorMessage="kafka: security protocol is not defined")

        if (protocol == "SSL") {
          kafkaSourceMap += "ssl.truststore.location" -> truststoreLocation
          kafkaSourceMap += "ssl.truststore.password" -> truststorePassword
          kafkaSourceMap += "ssl.keystore.location" -> truststoreLocation
          kafkaSourceMap += "ssl.keystore.password" -> truststorePassword
          kafkaSourceMap += "ssl.key.password" -> keyPassword
        }

        val sessionTimeout = if (pipelineOptionsMap.isDefined) pipelineOptionsMap.get.asInstanceOf[Map[String,Any]].getOrElse("sessionTimeout", "").toString else ""
        val heartbeatInterval = if (pipelineOptionsMap.isDefined) pipelineOptionsMap.get.asInstanceOf[Map[String,Any]].getOrElse("heartbeatInterval", "").toString else ""

        kafkaSourceMap += "session.timeout" -> checkEmptyValue(
          value = sessionTimeout,
          errorMessage = "kafka: session timeout is not defined")

        kafkaSourceMap += "heartbeat.interval" -> checkEmptyValue(
          value = heartbeatInterval,
          errorMessage = "kafka: heart beat interval is not defined")

      }
      case "event-hub" => {

        //set appConfig appType
        appConfig.appType = AppType.EVENTHUB_STREAMING_APP_TYPE


        //options
        // get connection for this source
        val conn = connectionIdConnectionMap(sourceConnectionId)
        val sourceConnectionMap = toMap(conn.connection).get.asInstanceOf[Map[String,Any]]

        // connectionStrings
        val connectionStringsMap = sourceConnectionMap("connectionStrings").asInstanceOf[Map[String,Any]]
        val namespace = connectionStringsMap.getOrElse("namespace", "").toString
        val saskeyname = connectionStringsMap.getOrElse("sharedAccessKeyName", "").toString
        val saskey = connectionStringsMap.getOrElse("sharedAccessKey", "").toString
        val entityname = connectionStringsMap.getOrElse("entityName", "").toString

        // get connection options
        val sourceOptionsMap = toMap(firstSource.options).get
        val connOptionsMap = toMap(conn.options).get

        // consumerGroup
        val consumerGroupConn = connOptionsMap.getOrElse("consumerGroup", "").toString
        val consumerGroup: String = sourceOptionsMap.getOrElse("consumerGroup", "").toString

        //autoOffSetReset
        val autoOffsetResetConn = connOptionsMap.getOrElse("autoOffsetReset", "").toString
        val autoOffsetReset = sourceOptionsMap.getOrElse("autoOffsetReset", "").toString

        val storeOffsetsPathConn = connOptionsMap.getOrElse("storeOffsetsPath", "").toString
        val storeOffsetsPath = sourceOptionsMap.getOrElse("storeOffsetsPath", "").toString

        val maxOffsetFilesConn = connOptionsMap.getOrElse("maxOffsetFiles", "").toString
        val maxOffsetFiles = sourceOptionsMap.getOrElse("maxOffsetFiles", "").toString

        val maxRatePerPartitionConn = connOptionsMap.getOrElse("maxRatePerPartition", "").toString
        val maxRatePerPartition = sourceOptionsMap.getOrElse("maxRatePerPartition", "").toString

        val sortDataByConn = connOptionsMap.getOrElse("sortDataBy", "").toString
        val sortDataBy = sourceOptionsMap.getOrElse("sortDataBy", "").toString

        val topicJsonPathConn = connOptionsMap.getOrElse("topicJsonPath", "").toString
        val topicJsonPath = sourceOptionsMap.getOrElse("topicJsonPath", "").toString

        //topics
        val topicList = conn.topics
        eventhubSourceMap += "topics" -> checkEmptyValue(
          valueFunc = () => { if (topicList.size > 0) topicList.mkString(",") else "" },
          errorMessage="eventhub: topics are not defined")

        // connection details
        eventhubSourceMap += "namespace" -> checkEmptyValue(
          value = namespace,
          errorMessage = "eventhub: namespace is not defined")

        eventhubSourceMap += "saskeyname" -> checkEmptyValue(
          value = saskeyname,
          errorMessage = "eventhub: saskeyname is not defined")

        eventhubSourceMap += "saskey" -> checkEmptyValue(
          value = saskey,
          errorMessage = "eventhub: saskey is not defined")

        eventhubSourceMap += "name" -> checkEmptyValue(
          value = entityname,
          errorMessage = "eventhub: entity name is not defined")

        // directory to store offsets
        val storeOffPth = if (storeOffsetsPath != "") storeOffsetsPath else storeOffsetsPathConn
        val appOffsetPath = HdfsUtils.getAppDir(storeOffPth, pipelineId)
        if (appOffsetPath.isDefined) {
          val dirName = appOffsetPath.get.toUri.getPath
          log.info(s"creating OFFSET directory: $dirName")
          HdfsUtils.createDirectory(dirName)
        }

        //options
        eventhubSourceMap += "consumer-group" -> checkEmptyValue (
          valueFunc = () => { if (consumerGroup != "") consumerGroup else consumerGroupConn },
          errorMessage = "eventhub: consumer group is not defined")

        eventhubSourceMap += "auto-offset-reset" -> checkEmptyValue(
          valueFunc = () => { if (autoOffsetReset != "") autoOffsetReset else autoOffsetResetConn },
          errorMessage = "eventhub: auto offset reset is not defined")

        eventhubSourceMap += "store-offsets-path" -> checkEmptyValue(
          value = appOffsetPath.get.toUri.getPath,
          errorMessage = "eventhub: store offsets path is not defined")

        eventhubSourceMap += "max-offset-files" -> {if (maxOffsetFiles != "") maxOffsetFiles else maxOffsetFilesConn}
        eventhubSourceMap += "max-offset-history-files" -> {if (maxOffsetFiles != "") maxOffsetFiles else maxOffsetFilesConn}
        eventhubSourceMap += "max-rate-per-partition" -> {if (maxRatePerPartition != "") maxRatePerPartition else maxRatePerPartitionConn}

        eventhubSourceMap += "sort-data-by" -> {if (sortDataBy != "") sortDataBy else sortDataByConn}
        eventhubSourceMap += "topic-json-path" -> {if (topicJsonPath != "") topicJsonPath else topicJsonPathConn}

        val sessionTimeout = if (pipelineOptionsMap.isDefined) pipelineOptionsMap.get.asInstanceOf[Map[String,Any]].getOrElse("sessionTimeout", "").toString else ""
        eventhubSourceMap += "receiver-timeout" -> checkEmptyValue(
          value = sessionTimeout,
          errorMessage = "eventhub: receiver timeout is not defined")

        eventhubSourceMap += "operation-timeout" -> checkEmptyValue(
          value = sessionTimeout,
          errorMessage = "eventhub: operation timeout is not defined")

      }
      case "hdfs-stream" => {

        //set appConfig appType
        appConfig.appType = AppType.HDFS_STREAMING_APP_TYPE

        //options
        // get connection for this source
        val conn = connectionIdConnectionMap(sourceConnectionId)
        val sourceConnectionMap = toMap(conn.connection).get.asInstanceOf[Map[String,Any]]

        // connectionStrings
        val connectionStringsMap = sourceConnectionMap.getOrElse("connectionStrings", Map[String,Any]()).asInstanceOf[Map[String,Any]]
        // nothing in connection strings for hdfs streaming

        // get connection options
        val sourceOptionsMap = toMap(firstSource.options).get
        val connOptionsMap = toMap(conn.options).get

        val pathConn = connOptionsMap.getOrElse("path", "").toString
        val path: String = sourceOptionsMap.getOrElse("path", "").toString

        val sinceConn = connOptionsMap.getOrElse("since", "").toString
        val since: String = sourceOptionsMap.getOrElse("since", "").toString

        val scratchDirConn = connOptionsMap.getOrElse("scratchDir", "").toString
        val scratchDir: String = sourceOptionsMap.getOrElse("scratchDir", "").toString

        val mergeSourceFilesConn = connOptionsMap.getOrElse("mergeSourceFiles", "").toString
        val mergeSourceFiles: String = sourceOptionsMap.getOrElse("mergeSourceFiles", "").toString

        val archiveConn = connOptionsMap.getOrElse("archive", "").toString
        val archive: String = sourceOptionsMap.getOrElse("archive", "").toString

        val filesBatchSizeConn = connOptionsMap.getOrElse("filesBatchSize", "").toString
        val filesBatchSize: String = sourceOptionsMap.getOrElse("filesBatchSize", "").toString

        val filesModifiedAgoConn = connOptionsMap.getOrElse("filesModifiedAgo", "").toString
        val filesModifiedAgo: String = sourceOptionsMap.getOrElse("filesModifiedAgo", "").toString

        val topicJsonPathConn = connOptionsMap.getOrElse("topicJsonPath", "").toString
        val topicJsonPath: String = sourceOptionsMap.getOrElse("topicJsonPath", "").toString

        val filenameRegexConn = connOptionsMap.getOrElse("filenameRegex", "").toString
        val filenameRegex: String = sourceOptionsMap.getOrElse("filenameRegex", "").toString

        val directoryRegexConn = connOptionsMap.getOrElse("directoryRegex", "").toString
        val directoryRegex: String = sourceOptionsMap.getOrElse("directoryRegex", "").toString

        val filesFormatConn = connOptionsMap.getOrElse("filesFormat", "").toString
        val filesFormat: String = sourceOptionsMap.getOrElse("filesFormat", "").toString

        val filesFormatCsvSeparatorConn = connOptionsMap.getOrElse("filesFormatCsvSeparator", "").toString
        val filesFormatCsvSeparator: String = sourceOptionsMap.getOrElse("filesFormatCsvSeparator", "").toString

        val filesFormatCsvInferSchemaConn = connOptionsMap.getOrElse("filesFormatCsvInferSchema", "").toString
        val filesFormatCsvInferSchema: String = sourceOptionsMap.getOrElse("filesFormatCsvInferSchema", "").toString

        //topics
        val topicList = conn.topics
        hdfsSourceMap += "topics" -> checkEmptyValue(
          valueFunc = () => { if (topicList.size > 0) topicList.mkString(",") else "" },
          errorMessage="hdfs-streaming: topics are not defined")


        // options
        hdfsSourceMap += "incoming" -> checkEmptyValue (
          valueFunc = () => { if (path != "") path else pathConn },
          errorMessage = "hdfs-streaming: path is not defined")

        hdfsSourceMap += "since" -> checkEmptyValue (
          valueFunc = () => { if (since != "") since else sinceConn },
          errorMessage = "hdfs-streaming: since is not defined")

        var scratchDirSet = {if (scratchDir != "") scratchDir else scratchDirConn}
        scratchDirSet = {if (scratchDirSet == "") "scratchDir" else scratchDirSet}
        // get app log dir, if path is relative construct path in users
        // home directory
        val appLogPath = HdfsUtils.getAppDir(scratchDirSet, pipelineId)
        if (appLogPath.isDefined) {
          val dirName = appLogPath.get.toUri.getPath
          log.info(s"creating SCRATCH directory: $dirName")
          HdfsUtils.createDirectory(dirName)
        }
        hdfsSourceMap += "scratch-dir" -> scratchDirSet

        // options
        hdfsSourceMap += "archive" -> {if (archive != "") archive else archiveConn}
        hdfsSourceMap += "files-batch-size" -> {if (filesBatchSize != "") filesBatchSize else filesBatchSizeConn}
        hdfsSourceMap += "files-modified-ago" -> {if (filesModifiedAgo != "") filesModifiedAgo else filesModifiedAgoConn}
        hdfsSourceMap += "topic-json-path" -> {if (topicJsonPath != "") topicJsonPath else topicJsonPathConn}
        hdfsSourceMap += "filename-regex" -> {if (filenameRegex != "") filenameRegex else filenameRegexConn}
        hdfsSourceMap += "directory-regex" -> {if (directoryRegex != "") directoryRegex else directoryRegexConn}

        // supported file format
        hdfsSourceMap += "files-format" -> {if (filesFormat != "") filesFormat else filesFormatConn}

        // csv options
        hdfsSourceMap += "files-format-csv-separator" -> {if (filesFormatCsvSeparator != "") filesFormatCsvSeparator else filesFormatCsvSeparatorConn}
        hdfsSourceMap += "files-format-csv-infer-schema" -> {if (filesFormatCsvInferSchema != "") filesFormatCsvInferSchema else filesFormatCsvInferSchemaConn}

      }
      case "blob-storage-stream" => {

        //set appConfig appType
        appConfig.appType = AppType.ABFS_STREAMING_APP_TYPE

        //options
        // get connection for this source
        val conn = connectionIdConnectionMap(sourceConnectionId)
        val sourceConnectionMap = toMap(conn.connection).get.asInstanceOf[Map[String,Any]]

        // connectionStrings
        val connectionStringsMap = sourceConnectionMap.getOrElse("connectionStrings", Map[String,Any]()).asInstanceOf[Map[String,Any]]
        //val url = connectionStringsMap.getOrElse("url", "").toString
        val storageAccount = connectionStringsMap.getOrElse("storageAccount", "").toString
        val container = connectionStringsMap.getOrElse("container", "").toString


        // get connection options
        val sourceOptionsMap = toMap(firstSource.options).get
        val connOptionsMap = toMap(conn.options).get

        val pathConn = connOptionsMap.getOrElse("path", "").toString
        val path: String = sourceOptionsMap.getOrElse("path", "").toString

        val sinceConn = connOptionsMap.getOrElse("since", "").toString
        val since: String = sourceOptionsMap.getOrElse("since", "").toString

        val scratchDirConn = connOptionsMap.getOrElse("scratchDir", "").toString
        val scratchDir: String = sourceOptionsMap.getOrElse("scratchDir", "").toString

        val mergeSourceFilesConn = connOptionsMap.getOrElse("mergeSourceFiles", "").toString
        val mergeSourceFiles: String = sourceOptionsMap.getOrElse("mergeSourceFiles", "").toString

        val archiveConn = connOptionsMap.getOrElse("archive", "").toString
        val archive: String = sourceOptionsMap.getOrElse("archive", "").toString

        val filesBatchSizeConn = connOptionsMap.getOrElse("filesBatchSize", "").toString
        val filesBatchSize: String = sourceOptionsMap.getOrElse("filesBatchSize", "").toString

        val filesModifiedAgoConn = connOptionsMap.getOrElse("filesModifiedAgo", "").toString
        val filesModifiedAgo: String = sourceOptionsMap.getOrElse("filesModifiedAgo", "").toString

        val topicJsonPathConn = connOptionsMap.getOrElse("topicJsonPath", "").toString
        val topicJsonPath: String = sourceOptionsMap.getOrElse("topicJsonPath", "").toString

        val filenameRegexConn = connOptionsMap.getOrElse("filenameRegex", "").toString
        val filenameRegex: String = sourceOptionsMap.getOrElse("filenameRegex", "").toString

        val directoryRegexConn = connOptionsMap.getOrElse("directoryRegex", "").toString
        val directoryRegex: String = sourceOptionsMap.getOrElse("directoryRegex", "").toString


        val filesFormatConn = connOptionsMap.getOrElse("filesFormat", "").toString
        val filesFormat: String = sourceOptionsMap.getOrElse("filesFormat", "").toString

        val filesFormatCsvSeparatorConn = connOptionsMap.getOrElse("filesFormatCsvSeparator", "").toString
        val filesFormatCsvSeparator: String = sourceOptionsMap.getOrElse("filesFormatCsvSeparator", "").toString

        val filesFormatCsvInferSchemaConn = connOptionsMap.getOrElse("filesFormatCsvInferSchema", "").toString
        val filesFormatCsvInferSchema: String = sourceOptionsMap.getOrElse("filesFormatCsvInferSchema", "").toString

        //set abfs storage account
        abfsSourceMap += "storage-account" -> checkEmptyValue(
          value = storageAccount,
          errorMessage = "abfs-streaming: storage account is not defined"
        )

        abfsSourceMap += "container" -> checkEmptyValue(
          value = container,
          errorMessage = "abfs-streaming: container is not defined"
        )

        //topics
        val topicList = conn.topics
        abfsSourceMap += "topics" -> checkEmptyValue(
          valueFunc = () => { if (topicList.size > 0) topicList.mkString(",") else "" },
          errorMessage="abfs-streaming: topics are not defined")


        // options
        abfsSourceMap += "incoming" -> checkEmptyValue (
          valueFunc = () => { if (path != "") path else pathConn },
          errorMessage = "abfs-streaming: path is not defined")

        abfsSourceMap += "since" -> checkEmptyValue (
          valueFunc = () => { if (since != "") since else sinceConn },
          errorMessage = "abfs-streaming: since is not defined")

        var scratchDirSet = {if (scratchDir != "") scratchDir else scratchDirConn}
        scratchDirSet = {if (scratchDirSet == "") "scratchDir" else scratchDirSet}
        // get app log dir, if path is relative construct path in users
        // home directory
        val appLogPath = HdfsUtils.getAppDir(scratchDirSet, pipelineId)
        if (appLogPath.isDefined) {
          val dirName = appLogPath.get.toUri.getPath
          log.info(s"creating SCRATCH directory: $dirName")
          HdfsUtils.createDirectory(dirName)
        }
        abfsSourceMap += "scratch-dir" -> scratchDirSet

        // options
        abfsSourceMap += "archive" -> {if (archive != "") archive else archiveConn}
        abfsSourceMap += "files-batch-size" -> {if (filesBatchSize != "") filesBatchSize else filesBatchSizeConn}
        abfsSourceMap += "files-modified-ago" -> {if (filesModifiedAgo != "") filesModifiedAgo else filesModifiedAgoConn}
        abfsSourceMap += "topic-json-path" -> {if (topicJsonPath != "") topicJsonPath else topicJsonPathConn}
        abfsSourceMap += "filename-regex" -> {if (filenameRegex != "") filenameRegex else filenameRegexConn}
        abfsSourceMap += "directory-regex" -> {if (directoryRegex != "") directoryRegex else directoryRegexConn}

        // supported file format
        abfsSourceMap += "files-format" -> {if (filesFormat != "") filesFormat else filesFormatConn}

        // csv options
        abfsSourceMap += "files-format-csv-separator" -> {if (filesFormatCsvSeparator != "") filesFormatCsvSeparator else filesFormatCsvSeparatorConn}
        abfsSourceMap += "files-format-csv-infer-schema" -> {if (filesFormatCsvInferSchema != "") filesFormatCsvInferSchema else filesFormatCsvInferSchemaConn}

      }
      case _ => {
        log.error(s"Unknown source of type ${firstSource.connectionType}, ignoring ...")
      }
    }


    // connections
    for (sink <- sinks) {

      val connectionId = sink.connectionId.get
      val connectionType = sink.connectionType.get

      log.warn("connectionId: " + connectionId)
      log.warn("connectionType: " + connectionType)

      connectionType match {

        case "hbase" =>  {

          // get connection for this sink
          val conn = connectionIdConnectionMap(connectionId)
          val sinkConnectionMap = toMap(conn.connection).get.asInstanceOf[Map[String,Any]]

          // connectionStrings
          val connectionStringsMap = sinkConnectionMap("connectionStrings").asInstanceOf[Map[String,Any]]
          val url = connectionStringsMap.getOrElse("url", "").toString
          val port = connectionStringsMap.getOrElse("port", "").toString

          // security
          val securityMap = sinkConnectionMap("security").asInstanceOf[Map[String,Any]]
          val protocol = securityMap.getOrElse("protocol", "").toString
          val kerberosPrincipal = securityMap.getOrElse("kerberosPrincipal", "").toString
          val kerberosKeytab = securityMap.getOrElse("kerberosKeytab", "").toString

          // get connection options
          val sinkOptionsMap = toMap(sink.options).get
          val connOptionsMap = toMap(conn.options).get

          // distributed mode
          val distributedModeConn = connOptionsMap.getOrElse("distributedMode", "").toString
          val distributedMode = sinkOptionsMap.getOrElse("distributedMode", "").toString

          // scanner caching
          val scannerCachingConn = connOptionsMap.getOrElse("scannerCaching", "").toString
          val scannerCaching = sinkOptionsMap.getOrElse("scannerCaching", "").toString

          hbaseSinkMap += "zookeeper.quorum" -> checkEmptyValue(
            value = url,
            errorMessage = "hbase: url is not defined")

          hbaseSinkMap += "zookeeper.client.port" -> checkEmptyValue(
            value = port,
            errorMessage = "hbase: port is not defined")

          if (protocol == "kerberos") {

            hbaseSinkMap += "kerberos.principal" -> checkEmptyValue(
              value = kerberosPrincipal,
              errorMessage = "hbase: security protocol is kerberos but kerberosPrincipal is not defined")

            hbaseSinkMap += "kerberos.keytab" -> checkEmptyValue(
              value = kerberosKeytab,
              errorMessage = "hbase: security protocol is kerberos but kerberosPrincipal is not defined")

          }

          //options
          hbaseSinkMap += "distributed.mode" -> checkEmptyValue(
            valueFunc = () => { if (distributedMode !="") distributedMode else distributedModeConn },
            errorMessage = "hbase: distributed mode is not defined")

          hbaseSinkMap += "scannerCaching" -> checkEmptyValue(
            valueFunc = () => { if (scannerCaching !="") scannerCaching else scannerCachingConn },
            errorMessage = "hbase: scanner caching is not defined")

        }

        case "hdfs-write" => {

          // get connection for this sink
          val conn = connectionIdConnectionMap(connectionId)
          val sinkConnectionMap = toMap(conn.connection).get.asInstanceOf[Map[String,Any]]

          // connectionStrings
          val connectionStringsMap = sinkConnectionMap("connectionStrings").asInstanceOf[Map[String,Any]]

          // get connection options
          val sinkOptionsMap = toMap(sink.options).get
          val connOptionsMap = toMap(conn.options).get

          // path
          val pathConn = connOptionsMap.getOrElse("path", "").toString
          val path = sinkOptionsMap.getOrElse("path", "").toString

          //storageFormat
          val storageFormatConn = connOptionsMap.getOrElse("storageFormatConn", "").toString
          val storageFormat = sinkOptionsMap.getOrElse("storageFormat", "").toString

          // suffix
          val fileSuffixConn = connOptionsMap.getOrElse("fileSuffix", "").toString
          val fileSuffix = sinkOptionsMap.getOrElse("fileSuffix", "").toString

          // enabled
          val enabledConn = connOptionsMap.getOrElse("enabled", "").toString
          val enabled = sinkOptionsMap.getOrElse("enabled", "").toString

          // mapping
          var storageMapConn = Map[String, Any]()
          val storageMapConnTry = Try { connOptionsMap.getOrElse("mapping", Map[String, Any]()).asInstanceOf[Map[String,Any]] }
          storageMapConnTry match {
            case Success(storageMap) => {
              storageMapConn = storageMap
            }
            case Failure(f) => {
              // parse list of map objects
              val storageMapConnOption = Option(parseStorageOptionsMappingList(connOptionsMap))
              storageMapConn = if (storageMapConnOption.isDefined) storageMapConnOption.get else Map[String, Any]()
            }
          }

          var storageMapSource = Map[String, Any]()
          val storageMapSourceTry =  Try { sinkOptionsMap.getOrElse("mapping", Map[String, Any]()).asInstanceOf[Map[String,Any]] }
          storageMapSourceTry match {
            case Success(storageMap) => {
              storageMapSource = storageMap
            }
            case Failure(f) => {
              // parse list of map objects
              val storageMapSourceOption = Option(parseStorageOptionsMappingList(sinkOptionsMap))
              storageMapSource = if (storageMapSourceOption.isDefined) storageMapSourceOption.get else Map[String, Any]()
            }
          }

          val hdfsEnabled = if (enabled != "") enabled else enabledConn
          storeRawMap += "enabled" -> checkEmptyValue(
            value = hdfsEnabled,
            errorMessage = "hdfs sink: enabled is not set")

          if (hdfsEnabled == "true") {

            // get store raw path
            val storeRawPth = checkEmptyValue(
              valueFunc = () => { if (path != "") path else pathConn},
              errorMessage="hdfs sink: path is not defined")

            val appRawPath = HdfsUtils.getAppDir(storeRawPth, pipelineId)
            if (appRawPath.isDefined) {
              val dirName = appRawPath.get.toUri.getPath
              log.info(s"creating RAW directory: $dirName")
              HdfsUtils.createDirectory(dirName)
            }

            storeRawMap += "path" -> checkEmptyValue(
              value = appRawPath.get.toUri.getPath,
              errorMessage = "hdfs sink: path is not defined")

            storeRawMap += "format" -> checkEmptyValue(
              valueFunc = () => { if (storageFormat != "") storageFormat else storageFormatConn },
              errorMessage = "hdfs sink: format is not defined")
          }

          // mapping
          if (storageMapSource.size > 0) {
            for ((topic, hdfsDirAny) <- storageMapSource) {
              storeRawMappingMap += topic -> hdfsDirAny.toString
            }
          } else {
            for ((topic, hdfsDirAny) <- storageMapConn) {
              storeRawMappingMap += topic -> hdfsDirAny.toString
            }
          }
        }

        case "eventhub-write" =>  {

          // eventhub sink
          val conn = connectionIdConnectionMap(connectionId)
          val sinkConnectionMap = toMap(conn.connection).get.asInstanceOf[Map[String,Any]]

          // connectionStrings
          val connectionStringsMap = sinkConnectionMap("connectionStrings").asInstanceOf[Map[String,Any]]
          val namespace = connectionStringsMap.getOrElse("namespace", "").toString
          val saskeyname = connectionStringsMap.getOrElse("sharedAccessKeyName", "").toString
          val saskey = connectionStringsMap.getOrElse("sharedAccessKey", "").toString
          val entityname = connectionStringsMap.getOrElse("entityName", "").toString

          // get connection options
          val sinkOptionsMap = toMap(sink.options).get
          val connOptionsMap = toMap(conn.options).get

          // connection details
          eventhubSinkMap += "namespace" -> checkEmptyValue(
          value = namespace,
          errorMessage = "eventhub-write: namespace is not defined")

          eventhubSinkMap += "saskeyname" -> checkEmptyValue(
            value = saskeyname,
            errorMessage = "eventhub-write: saskeyname is not defined")

          eventhubSinkMap += "saskey" -> checkEmptyValue(
            value = saskey,
            errorMessage = "eventhub-write: saskey is not defined")

          eventhubSinkMap += "name" -> checkEmptyValue(
            value = entityname,
            errorMessage = "eventhub-write: entity name is not defined")

          val sessionTimeout = if (pipelineOptionsMap.isDefined) pipelineOptionsMap.get.asInstanceOf[Map[String,Any]].getOrElse("sessionTimeout", "").toString else ""
          eventhubSinkMap += "operation-timeout" -> checkEmptyValue(
            value = sessionTimeout,
            errorMessage = "eventhub: session timeout is not defined")

          // enabled
          val enabledConn = connOptionsMap.getOrElse("enabled", "").toString
          val enabled = sinkOptionsMap.getOrElse("enabled", "").toString
          val eventWriteEnabled = if (enabled != "") enabled else enabledConn
          eventhubSinkMap += "enabled" -> checkEmptyValue(
            value = eventWriteEnabled,
            errorMessage = "eventhub-write: enabled is not set")
        }

        case _ => {
          log.info(s"Unknown sink of type  $connectionType, ignoring ...")
        }
      }
    }

    // operator
    firstOperation.operatorType.get match {
      case "pythonRules" => {
        //set appConfig appType
        appConfig.appType = AppType.OB_RULES_APP_TYPE

        if (operationOptionsMap.isDefined) {
          val operationOptions = operationOptionsMap.get
          val ruleTestPath = operationOptions.getOrElse("path", "").toString
          val resultPath = operationOptions.getOrElse("resultPath", "").toString
          val dependenciesFolder = operationOptions.getOrElse("dependenciesFolder", "").toString

          ruleTestConfigMap += "root-path" -> ruleTestPath
          ruleTestConfigMap += "since" -> "2020-04-20T00:00:00" //TODO: minhdong Need a since option in pipeline config

          ruleLogConfigMap += "path" -> resultPath
          rulesConfigMap += "mode" -> "files"
          if (firstOperation.filesArray.isDefined) {
            rulesConfigMap += "files" -> firstOperation.files.mkString(",")
          }
          var orderObjectMap = Map[String, String]()
          var deliveryObjectMap = Map[String, String]()
          orderObjectMap += "files" -> firstOperation.files.mkString(",")
          orderObjectMap += "mode" -> "files"
          orderObjectMap += "dependency-path" -> dependenciesFolder
          orderObjectMap += "results-path" -> resultPath

          deliveryObjectMap += "files" -> firstOperation.files.mkString(",")
          deliveryObjectMap += "mode" -> "files"
          deliveryObjectMap += "dependency-path" -> dependenciesFolder
          deliveryObjectMap += "results-path" -> resultPath

          uberRulesConfigMap += "OrderObject" -> orderObjectMap
          uberRulesConfigMap += "DeliveryObject" -> deliveryObjectMap
        }

        appConfig.ruletestConfigMap = ruleTestConfigMap
        appConfig.rulesConfigMap = rulesConfigMap
        appConfig.ruleLogConfigMap = ruleLogConfigMap
        appConfig.uberRulesConfigMap = uberRulesConfigMap
      }
      case _ => {
        log.error(s"Unknown operator type ${firstOperation.operatorType}, ignoring ...")
      }
    }


    // appConfig
    appConfig.appConfigMap = appMap
    appConfig.sparkConfigMap = sparkMap
    appConfig.messageMapperConfigMap = messageMap
    appConfig.auditLogConfigMap = auditLogMap

    // source
    appConfig.kafkaConfigMap = kafkaSourceMap
    appConfig.eventhubConfigMap = eventhubSourceMap
    appConfig.hdfsStreamingConfigMap = hdfsSourceMap
    appConfig.abfsStreamingConfigMap = abfsSourceMap

    // sinks
    appConfig.hbaseConfigMap = hbaseSinkMap
    appConfig.storeRawConfigMap = storeRawMap
    appConfig.storeRawMappingConfigMap = storeRawMappingMap
    appConfig.eventhubWriteConfigMap = eventhubSinkMap

    appConfig
  }

  /** get a map of key value pairs, and an empty map if the field wasnt available
    *
    * @param field - specified field which contains key value pairs
    * @returns [[Option[Map[String, String]]]]  containing key value pairs
    */
  def toMap(jsonObject: Option[JSONObject]): Option[Map[String, Any]] = {

    var mm = Map[String, Any]()
    jsonObject match {
      case Some(jobj) => {
        var iter = jobj.keySet().iterator()
        while (iter.hasNext()) {
          val k = iter.next()
          val v = jobj.get(k)
          if (v.isInstanceOf[String]) {
            mm += k.asInstanceOf[String] -> v.asInstanceOf[String]
          } else if (v.isInstanceOf[Boolean]) {
            mm += k.asInstanceOf[String] -> v.asInstanceOf[Boolean]
          } else if (v.isInstanceOf[Int]) {
            mm += k.asInstanceOf[String] -> v.asInstanceOf[Int]
          } else if (v.isInstanceOf[JSONArray]) {
            val jsonArray = v.asInstanceOf[JSONArray]
            val n: Int = jsonArray.size
            val jsonList: List[Any] = (0 to (n-1)).map(i => {
              val elem = jsonArray.get(i)
              if (elem.isInstanceOf[String]) {
                jsonArray.get(i).asInstanceOf[String]
              } else if (elem.isInstanceOf[Boolean]) {
                jsonArray.get(i).asInstanceOf[Boolean]
              } else if (elem.isInstanceOf[JSONObject]) {
                val elemJsonObject = elem.asInstanceOf[JSONObject]
                val elemSubMap = toMap(Some(elemJsonObject))
                if (elemSubMap.isDefined) {
                  elemSubMap.get
                }
              }
            }).toList
            mm += k.asInstanceOf[String] -> jsonList
          } else if (v.isInstanceOf[JSONObject]) {
            val jsonObject = v.asInstanceOf[JSONObject]
            val subMap = toMap(Some(jsonObject))
            if (subMap.isDefined) {
              mm += k.asInstanceOf[String] -> subMap.get
            }
          } else {
            mm += k.asInstanceOf[String] -> v.toString
          }

        }
        Some(mm)
      }
      case None => {
        Some(mm)
      }
    }
  }
}
