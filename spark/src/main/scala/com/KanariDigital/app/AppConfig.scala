/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app

import java.io.InputStream

import com.KanariDigital.app.util.LogTag
import com.kanaridi.common.util.LogUtils
import com.KanariDigital.JsonConfiguration.JsonConfig

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

class AppConfig extends Serializable {

  val log: Log = LogFactory.getLog("AppConfig")

  //config path
  var configPath: String = ""

  //runtime app id
  var runtimeAppId: String = ""

  //app id
  var appId: String = ""

  //app name
  var appName: String = ""

  //app type
  var appType: String = ""

  //various config key value pairs
  var appConfigMap: Map[String, String] = Map.empty[String, String]
  var eventhubConfigMap: Map[String, String] = Map.empty[String, String]
  var kafkaConfigMap: Map[String, String] = Map.empty[String, String]
  var sparkConfigMap: Map[String, String] = Map.empty[String, String]
  var auditLogConfigMap: Map[String, String] = Map.empty[String, String]
  var hdfsLogConfigMap: Map[String, String] = Map.empty[String, String]
  var storeRawConfigMap: Map[String, String] = Map.empty[String, String]
  var storeRawMappingConfigMap: Map[String, String] = Map.empty[String, String]
  var hbaseConfigMap: Map[String, String] = Map.empty[String, String]
  var jdbcConfigMap: Map[String, String] = Map.empty[String, String]
  var graphiteConfigMap: Map[String, String] = Map.empty[String, String]
  var validationMapperConfigMap: Map[String, String] = Map.empty[String, String]
  var ruleLogConfigMap: Map[String, String] = Map.empty[String, String]
  var uberRulesConfigMap:  Map[String, Map[String, String]] = Map.empty[String, Map[String, String]]
  var rulesConfigMap: Map[String, String] = Map.empty[String, String]
  var ruletestConfigMap: Map[String, String] = Map.empty[String, String]
  var messageMapperConfigMap: Map[String, String] = Map.empty[String, String]
  var heartBeatConfigMap: Map[String, String] = Map.empty[String, String]
  var hdfsStreamingConfigMap: Map[String, String] = Map.empty[String, String]
  var abfsStreamingConfigMap: Map[String, String] = Map.empty[String, String]
  var eventhubWriteConfigMap: Map[String, String] = Map.empty[String, String]
  var eventhubWriteMappingConfigMap: Map[String, Map[String, String]] = Map.empty[String, Map[String, String]]
  var hdfsWriteConfigMap: Map[String, String] = Map.empty[String, String]
  var hdfsWriteMappingConfigMap: Map[String, String] = Map.empty[String, String]

  /** get application id, if application id is not specified return application name */
  def getApplicationId(): String = {
    val applicationId = if (appId != "") appId else appName
    applicationId
  }

  def setRulesConfigMapField(key: String, value: String): Unit = {
    val mutablerulesConfigMap = scala.collection.mutable.Map[String, String]() ++= rulesConfigMap
    mutablerulesConfigMap(key) = value
    rulesConfigMap = mutablerulesConfigMap.toMap
  }

  /** convert [[AppConfig]] object to string for debugging/logging purposes
    *
    *  @returns string representation of [[AppConfig]] object
    */
  override def toString() : String = {

    val runtimeAppIdStr =  runtimeAppId.toString
    val appIdStr =  appId.toString
    val appNameStr =  appName.toString
    val appTypeStr =  appType.toString
    val configPathStr = configPath.toString

    val appConfigMapStr = AppConfig.mapToString(appConfigMap)
    val eventhubConfigMapStr = AppConfig.mapToString(eventhubConfigMap)
    val kafkaConfigMapStr = AppConfig.mapToString(kafkaConfigMap)
    val sparkConfigMapStr = AppConfig.mapToString(sparkConfigMap)
    val auditLogConfigMapStr = AppConfig.mapToString(auditLogConfigMap)
    val hdfsLogConfigMapStr = AppConfig.mapToString(hdfsLogConfigMap)
    val storeRawConfigMapStr = AppConfig.mapToString(storeRawConfigMap)
    val storeRawMappingConfigMapStr = AppConfig.mapToString(storeRawMappingConfigMap)
    val hbaseConfigMapStr = AppConfig.mapToString(hbaseConfigMap)
    val jdbcConfigMapStr = AppConfig.mapToString(jdbcConfigMap)
    val graphiteConfigMapStr = AppConfig.mapToString(graphiteConfigMap)
    val validationMapperConfigMapStr = AppConfig.mapToString(validationMapperConfigMap)
    val rulesLogConfigMapStr = AppConfig.mapToString(ruleLogConfigMap)
    val rulesMapStr = uberRulesConfigMap.toString()
    val rulesConfigMapStr = AppConfig.mapToString(rulesConfigMap)
    val messageMapperConfigMapStr = AppConfig.mapToString(messageMapperConfigMap)
    val ruletestConfigMapStr = AppConfig.mapToString(ruletestConfigMap)
    val heartBeatConfigMapStr = AppConfig.mapToString(heartBeatConfigMap)
    val hdfsStreamingConfigMapStr = AppConfig.mapToString(hdfsStreamingConfigMap)
    val abfsStreamingConfigMapStr = AppConfig.mapToString(abfsStreamingConfigMap)
    val eventhubWriteConfigMapStr = AppConfig.mapToString(eventhubWriteConfigMap)
    val eventhubWriteMappingConfigMapStr = eventhubWriteMappingConfigMap.toString
    val hdfsWriteConfigMapStr = AppConfig.mapToString(hdfsWriteConfigMap)
    val hdfsWriteMappingConfigMapStr = AppConfig.mapToString(hdfsWriteMappingConfigMap)

    s""" AppConfig: [ runtime-app-id: $runtimeAppIdStr
                    [ app-id: $appIdStr
                    | app-name: $appNameStr
                    | config-path: $configPathStr
                    | appType: $appTypeStr
                    | app: $appConfigMapStr
                    | eventhub: $eventhubConfigMapStr
                    | kafka: $kafkaConfigMapStr
                    | spark: $sparkConfigMapStr
                    | audit-log: $auditLogConfigMapStr
                    | hdfs-log: $hdfsLogConfigMapStr
                    | store-raw: $storeRawConfigMapStr
                    | store-raw-mapping: $storeRawMappingConfigMapStr
                    | hbase: $hbaseConfigMapStr
                    | jdbc: $jdbcConfigMapStr
                    | graphite: $graphiteConfigMapStr
                    | validation-mapper: $validationMapperConfigMapStr
                    | rule-source: $rulesConfigMapStr
                    | rule-log: $rulesLogConfigMapStr
                    | rules: $rulesMapStr
                    | rule-test: $ruletestConfigMapStr
                    | message-mapper: $messageMapperConfigMapStr
                    | heartbeat: $heartBeatConfigMapStr
                    | hdfs-streaming: $hdfsStreamingConfigMapStr
                    | abfs-streaming: $abfsStreamingConfigMapStr
                    | eventhub-write: $eventhubWriteConfigMapStr
                    | eventhub-write-mapping: $eventhubWriteMappingConfigMapStr
                    | hdfs-write: $hdfsWriteConfigMapStr
                    | hdfs-write-mapping: $hdfsWriteMappingConfigMapStr]
     """.stripMargin
  }
}

object AppConfig {

  val log: Log = LogFactory.getLog(AppConfig.getClass.getName)

  /** converts Map[String, String]] object to a string
    * {{
    * key1 => value1, key2 => value2
    * }}
    */
  def mapToString(myMap: Map[String, String]): String = {
    val mapStr = myMap.map { case(key, value) => s"$key=$value" }.mkString(", ")
    mapStr
  }

  /** read json config file for specified appName
    *
    *  @param appName application name
    *  @param inputStream json config file input stream
    *  @return [[AppConfig]] object
    */
  def readJsonFile(appName: String, inputStream: InputStream): AppConfig = {

    val jsonConfig = new JsonConfig

    //read config for given appName
    jsonConfig.read(inputStream, Some(appName))

    var appConfig = new AppConfig

    appConfig.appName = jsonConfig.getStringValue("app-name").getOrElse("undefined")
    appConfig.appType = jsonConfig.getStringValue("app-type").getOrElse("undefined")

    // app (required)
    appConfig.appConfigMap = jsonConfig.getMapValue("app").get

    // read data (optional)
    appConfig.eventhubConfigMap = jsonConfig.getMapValue("eventhub").getOrElse(Map.empty[String, String])
    appConfig.kafkaConfigMap = jsonConfig.getMapValue("kafka").getOrElse(Map.empty[String, String])
    appConfig.hdfsStreamingConfigMap = jsonConfig.getMapValue("hdfs-streaming").getOrElse(Map.empty[String, String])
    appConfig.abfsStreamingConfigMap = jsonConfig.getMapValue("abfs-streaming").getOrElse(Map.empty[String, String])

    // spark
    appConfig.sparkConfigMap = jsonConfig.getMapValue("spark").get

    // log (required)
    appConfig.auditLogConfigMap = jsonConfig.getMapValue("audit-log").get
    appConfig.hdfsLogConfigMap = jsonConfig.getMapValue("hdfs-log").getOrElse(Map.empty[String, String])

    // store raw (optional)
    appConfig.storeRawConfigMap = jsonConfig.getMapValue("store-raw").getOrElse(Map.empty[String, String])
    appConfig.storeRawMappingConfigMap = jsonConfig.getMapValue("store-raw-mapping").getOrElse(Map.empty[String, String])

    // write data (optional)
    appConfig.hbaseConfigMap = jsonConfig.getMapValue("hbase").get
    appConfig.jdbcConfigMap = jsonConfig.getMapValue("jdbc").get
    appConfig.graphiteConfigMap = jsonConfig.getMapValue("graphite").get
    appConfig.validationMapperConfigMap = jsonConfig.getMapValue("validation-mapper").get
    appConfig.eventhubWriteConfigMap = jsonConfig.getMapValue("eventhub-write").getOrElse(Map.empty[String, String])
    appConfig.eventhubWriteMappingConfigMap = jsonConfig.getNestedMap("eventhub-write-mapping").getOrElse(Map.empty[String, Map[String, String]])
    appConfig.hdfsWriteConfigMap = jsonConfig.getMapValue("hdfs-write").getOrElse(Map.empty[String, String])
    appConfig.hdfsWriteMappingConfigMap = jsonConfig.getMapValue("hdfs-write-mapping").getOrElse(Map.empty[String, String])

    // rules (required)
    appConfig.ruletestConfigMap = jsonConfig.getMapValue("rule-test").get
    appConfig.rulesConfigMap = jsonConfig.getMapValue("rule-source").get
    appConfig.ruleLogConfigMap = jsonConfig.getMapValue("rule-log").get
    appConfig.uberRulesConfigMap = jsonConfig.getNestedMap("rules").getOrElse(Map.empty[String, Map[String, String]])

    // mapper (optional)
    appConfig.messageMapperConfigMap = jsonConfig.getMapValue("message-mapper").getOrElse(Map.empty[String, String])

    // heartbeat (optional)
    appConfig.heartBeatConfigMap = jsonConfig.getMapValue("heart-beat").getOrElse(Map.empty[String, String])

    appConfig
  }

  def readJsonFile(appName: String, path : String, config: HadoopConfiguration ) : AppConfig = {
    val fileSystem = FileSystem.get(config)
    val inputStream = fileSystem.open(new Path(path))
    val eventHubConfig = readJsonFile(appName, inputStream)
    inputStream.close()
    eventHubConfig
  }

  /** read app configuration section from config file */
  def readAppConfig(appName: String, path: String, hadoopConfiguration: HadoopConfiguration): AppConfig = {

    log.info(s"readAppConfig: Reading AppConfig JSON configuration file $path")

    Try(AppConfig.readJsonFile(appName, path, hadoopConfiguration)) match {
      case Success(theConfig) => {
        val configStr = theConfig.toString()
        log.info(s"readAppConfig: appName: $appName - Loaded config: $configStr")
        theConfig
      }
      case Failure(ex) => {

        ex.printStackTrace

        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""${LogTag.APPLICATION_ID_NONE} - ${LogTag.FATAL_APPCONFIG_PARSE_ERROR} -
                  |appName: $appName, path: $path,
                  |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
                  |message: bailing out have run into fatal error parsing config file with message $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        throw ex
      }
    }
  }

  /** read pipeline configuration section from config file */
  def readPipelineConfig(path: String, hadoopConfiguration: HadoopConfiguration): PipelineConfig = {

    log.info(s"readPipelineConfig: Reading PipelineConfig JSON configuration file $path")

    Try(PipelineConfig.readJsonFile(path, hadoopConfiguration)) match {
      case Success(theConfig) => {
        val configStr = theConfig.toString()
        log.info(s"readPipelineConfig: Loaded config: $configStr")
        theConfig
      }
      case Failure(ex) => {

        // ex.printStackTrace

        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""${LogTag.APPLICATION_ID_NONE} - ${LogTag.FATAL_PIPELINECONFIG_PARSE_ERROR} -
                  |path: $path,
                  |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
                  |message: bailing out have run into fatal error parsing pipeline config file $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        throw ex
      }
    }
  }

  /** read config file as a pipeline config or an app config  */
  def readPipelineorAppConfig(configPath: String, appNameOption: Option[String], hadoopConfiguration: HadoopConfiguration): AppConfig = {

    //try pipeline config
    val pipelineConfigTry = Try(readPipelineConfig(configPath, hadoopConfiguration))
    pipelineConfigTry match {
      case Success(pipelineConfig) => {
        log.info(s"$configPath is in pipeline config format")
        var appConfigTry = Try(PipelineConfig.convertToAppConfig(pipelineConfig))
        appConfigTry match {
          case Success(appConfig) => {
            appConfig.configPath = configPath
            appConfig
          }
          case Failure(ex) => {

            ex.printStackTrace

            val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

            log.error(s"""${LogTag.APPLICATION_ID_NONE} - ${LogTag.FATAL_PIPELINECONFIG_CONVERT_ERROR} -
                  |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
                  |message: bailing out have run into fatal error converting pipeline config to app config with message $exMesg"""
              .stripMargin.replaceAll("\n", " "))

            throw ex
          }
        }
      }
      case Failure(ex) => {
        // ex.printStackTrace
        log.error(s"$configPath is not a pipeline config, trying app config")
        val appName:String = appNameOption.getOrElse("")
        Try(readAppConfig(appName, configPath, hadoopConfiguration)) match {
          case Success(appConfig) =>  {
            appConfig.configPath = configPath
            appConfig
          }
          case Failure(f) => {

            f.printStackTrace

            throw f
          }
        }
      }
    }
  }


}
