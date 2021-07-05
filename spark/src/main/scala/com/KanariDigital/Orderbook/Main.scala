/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook

import com.KanariDigital.JsonConfiguration.JsonConfig
import com.KanariDigital.KafkaConsumer.{KafkaConfiguration, KafkaConsumerFactory, StreamingContextFactory}
import com.KanariDigital.app.AuditLog.{HdfsLogger, HdfsLoggerConfig, HeartBeatLogger}
import com.KanariDigital.Orderbook.HdfsHiveWriter._
import com.KanariDigital.app.util.SparkUtils
import com.KanariDigital.hbase.HBaseConfiguration
import com.KanariDigital.hbase.HBaseConfiguration.readJsonFile
import com.kanaridi.xform._
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success}


object Main {

  val log: Log = LogFactory.getLog(Main.getClass.getName)

  var appNameOption: Option[String] = None
  var configPath : Option[String] = None
  var isLocal = false
  var readFrom: String = "201906"
  var startDate : String = "20191128"

  /** Contains regular expressions to parse command line options */
  object CommandLineArguments {

    val appNameRegEx = "-appName=(.*)".r
    val readFromRegEx = "-readFrom=(.*)".r
    val startDateRegEx = "-startDate=(.*)".r

    val hdfsConfigXmlRegEx = "-resource=(.*)".r
    val configRegEx = "-config=(.*)".r
    val isLocalFlag = "-local"

  }

  // An component to inject into the stream processor
  class ConcreteHdfsWriterFactory(configPath : Option[String]) extends HdfsHiveWriterFactoryTrait with Serializable {
    override def createHdfsHiveWriter(): Option[HdfsHiveWriter] = {

      if (configPath.isDefined == false) {
        log.info("ConcreteHdfsHiveWriterFactory: createHdfsHiveWriter:"
          + " cannot create a writer config path is not defined")
        return None
      }

      log.info("ConcreteHdfsHiveWriterFactory: createHdfsHiveWriter: configPath: "
        + configPath.get)

      val config = readHdfsHiveConfig(configPath.get)

      if (config.isDefined == false) {
        log.info("ConcreteHdfsHiveWriterFactory: createHdfsHiveWriter: config not defined")
        return None
      }

      if (config.get.enabled == false) {
        log.info("ConcreteHdfsHiveWriterFactory: createHdfsHiveWriter: enabled is set to false")
        return None
      }
      HdfsHiveWriterFactory.construct(config.get, sqlContext) match {
        case Success(writer) => {
          log.info("ConcreteHdfsHiveWriterFactory: createHdfsHiveWriter: success")
          Some(writer)
        }
        case Failure(ex) => {
          log.error("ConcreteHdfsHiveWriterFactory: createHdfsHiveWriter: failure")
          None
        }
      }
    }
  }

  val hadoopConfiguration = new HadoopConfiguration()

  var kafkaConfiguration : Option[KafkaConfiguration] = None
  var hBaseConfiguration : Option[HBaseConfiguration] = None
  var hdfsHiveWriter : Option[HdfsHiveWriter] = None
  var sqlContext : SQLContext = null
  var auditLogConfig : Option[HdfsLoggerConfig] = None
  var hdfsLogConfig : Option[HdfsLoggerConfig] = None
  var heartBeatLogger : Option[HeartBeatLogger] = None
  var xform : Option[MessageMapper] = None
  var obAppConfig : Option[OBAppConfiguration] = None

  def readKafkaConfiguration(path: String): Boolean = {

    log.trace(s"Reading Kafka JSON configuration file $path")
    KafkaConfiguration.readJsonFile(path, hadoopConfiguration) match {
      case Success(theConfig) => {
        kafkaConfiguration = Some(theConfig)
        val configStr = theConfig.toString()
        log.info(s"Loaded kafka configuration $configStr")
        val valid = theConfig.isValid()
        if (valid == false) {
          val reasons = theConfig.validationReasons().mkString(", ")
          log.error(s"Kafka Configuration is NOT valid. Reasons: $reasons")
        }
        valid
      }
      case Failure(f) => {
        val exc = f.getMessage
        log.error(s"Unable to load Kafka configuration from $path - Exception: $exc")
        false
      }
    }
  }

  def readHbaseConfig(configPath: String): Boolean = {
    val hadoopConfiguration = new HadoopConfiguration()
    val tryConfig = readJsonFile(configPath, hadoopConfiguration)

    tryConfig match {
      case Success(c) => {
        hBaseConfiguration = Some(c)
        true
      }
      case Failure(ex) => {
        val message = ex.getMessage()
        log.error(s"Error reading HBase configuration: $message")
        false
      }
    }
  }

  def readHdfsHiveConfig(path: String): Option[HdfsHiveWriterConfig] = {
    log.trace(s"Reading HDFS Hive JSON configuration file $path")
    HdfsHiveWriterConfig.readJsonFile(path, hadoopConfiguration) match {
      case Success(c) => {
        log.info("HDFS Hive Config: " + c.toString())
        Some(c)
      }

      case Failure(ex) => {
        val msg = ex.getMessage()
        log.error("Unable to read configuration, exception: $msg")
        None
      }
    }
  }

  def readAuditLogConfig(path:String) : Boolean = {
    log.trace(s"Reading Audit Log Configuration from $path")
    HdfsLoggerConfig.readJsonFile(path, "audit-log", hadoopConfiguration) match {
      case Success(c) => {
        log.info(s"Audit log configuration - Path = '$path'")
        auditLogConfig = Some(c)
        true
      }
      case Failure(ex) => {
        val mesg = ex.getMessage
        log.error(s"Failed to read Audit Log Config: $mesg")
        false
      }
    }
  }

  def readHdfsLoggerConfig(path:String) : Boolean = {
    log.trace(s"Reading Audit Log Configuration from $path")
    HdfsLoggerConfig.readJsonFile(path, "hdfs-log", hadoopConfiguration) match {
      case Success(c) => {
        log.info(s"HDFS log configuration - Path = '$path'")
        hdfsLogConfig = Some(c)
        true
      }
      case Failure(ex) => {
        val mesg = ex.getMessage
        log.error(s"Failed to read HDFS Log Config: $mesg")
        false
      }
    }
  }

  def readHeartBeatConfig(path : String) : Boolean = {
    log.trace(s"Reading heartbeat configuration")
    val fileSystem = FileSystem.get(hadoopConfiguration)
    val inputStream = fileSystem.open(new Path(path))
    val jsonConfig = new JsonConfig
    try {
      jsonConfig.read(inputStream, Some("heart-beat"))
      val path = jsonConfig.getStringValue("path")
      val sec = jsonConfig.getLongValue("interval-sec")
      if (path.isDefined && sec.isDefined) {
        log.info(s"Creating heart beat logger at path: ${path.get} and interval of ${sec.get} seconds.")
        val hb = new HeartBeatLogger(path.get)
        hb.enable(sec.get)
        heartBeatLogger = Some(new HeartBeatLogger(path.get))
      } else {
        log.info("Heart beat configuration not present or incomplete.")
        return false
      }
      return true
    } catch {
      case ex: Exception => {
        log.error(s"Error encountered with Heart Beat configuration: ${ex.toString()}")
        return false
      }
    } finally {
      inputStream.close()
    }
  }

  def readMapperConfig(path : String) : Boolean = {
    log.trace(s"Reading mapper configuration")
    val fileSystem = FileSystem.get(hadoopConfiguration)
    val inputStream = fileSystem.open(new Path(path))
    val jsonConfig = new JsonConfig
    try {
      jsonConfig.read(inputStream, Some("message-mapper"))
      val path = jsonConfig.getStringValue("path")
      if (path.isDefined) {
        log.info(s"Creating MessageMapper with path: ${path.get}.")
        val mmcStream = fileSystem.open(new Path(path.get))
        val mmcfg = MessageMapperConfig(mmcStream)
        xform = Some(MessageMapperFactory(mmcfg))

      } else {
        log.info("MessageMapper config not present or incomplete.")
        return false
      }
      return true
    } catch {
      case ex: Exception => {
        log.error(s"Error encountered with mapper configuration: ${ex.toString()}")
        return false
      }
    } finally {
      inputStream.close()
    }
  }

  /** read application configuration from ob-app section in the config file */
  def readOBAppConfig(path : String) : Boolean = {
    log.trace(s"Reading ob application configuration")
    val fileSystem = FileSystem.get(hadoopConfiguration)
    val inputStream = fileSystem.open(new Path(path))
    val jsonConfig = new JsonConfig
    try {
      jsonConfig.read(inputStream, Some("ob-app"))
      val enableDedup = jsonConfig.getStringValue("enable-dedup")
      val dedupColFamily = jsonConfig.getStringValue("dedup-colfamily")
      val dedupColName = jsonConfig.getStringValue("dedup-colname")
      val dedupLookback = jsonConfig.getStringValue("dedup-lookback")
      val maxBatchSize = jsonConfig.getStringValue("max-batch-size")
      val defaultStorageLevel = jsonConfig.getStringValue("default-storage-level")
      val fusionAvroPath = jsonConfig.getStringValue("fusion-avro-path")

      if (enableDedup.isDefined) {

        // create a new object
        var obAppConfiguration = new OBAppConfiguration

        obAppConfiguration.enableDedup = enableDedup
        obAppConfiguration.dedupColFamily = dedupColFamily
        obAppConfiguration.dedupColName = dedupColName
        obAppConfiguration.dedupLookback = dedupLookback
        obAppConfiguration.maxBatchSize = maxBatchSize
        obAppConfiguration.defaultStorageLevel = defaultStorageLevel
        obAppConfiguration.fusionAvroPath = fusionAvroPath
        obAppConfig = Some(obAppConfiguration)

        // debug
        val configStr = obAppConfig.toString()
        log.info(s"Loaded OBAppConfig: $configStr")

        //check if config is valid
        val valid = obAppConfig.get.isValid()
        if (valid == false) {
          val reasons = obAppConfig.get.validationReasons().mkString(", ")
          log.error(s"OBAppConfiguration is NOT valid. Reasons: $reasons")
        }

        valid
      } else {
        log.info("OBAppConfiguration not present or incomplete.")
        return false
      }
      return true
    } catch {
      case ex: Exception => {
        log.error(s"Error encountered with OBAppConfiguration: ${ex.toString()}")
        return false
      }
    } finally {
      inputStream.close()
    }
  }

  def readConfigurationSections(path: String): Boolean = {
    log.info(s"Reading configuration from '$path'.")
    val success = readKafkaConfiguration(path) &&
    readHbaseConfig(path) &&
    readAuditLogConfig(path) &&
    readHdfsLoggerConfig(path) &&
    readHeartBeatConfig(path) &&
    readMapperConfig(path) &&
    readOBAppConfig(path)
    success
  }

  /** parse commandline arguments using regular expressions
    * specified in [[CommandLineArguments]]
    */
  def processArg(arg: String): Boolean = {

    log.info(s"arg = $arg")

    arg match {

      case CommandLineArguments.appNameRegEx(appStr) => {
        appNameOption = Some(appStr)
        true
      }

      case CommandLineArguments.readFromRegEx(rfStr) => {
        readFrom = rfStr
        true
      }

      case CommandLineArguments.startDateRegEx(sdStr) => {
        startDate = sdStr
        true
      }

      case CommandLineArguments.hdfsConfigXmlRegEx(xml) => {
        hadoopConfiguration.addResource(xml)
        true
      }

      case CommandLineArguments.configRegEx(path) => {
        configPath = Some(path)
        true
      }

      case CommandLineArguments.isLocalFlag => {
        isLocal = true
        true
      }

      case _ => {
        log.error(s"Unable process argument $arg. Exiting.....")
        false
      }
    }
  }

  //  def testSerialization(processor: RddProcessor) : Unit = {
  //    val bos = new ByteArrayOutputStream()
  //    val oos = new ObjectOutputStream(bos)
  //    oos.writeObject(processor)
  //    oos.close()
  //    val sz = bos.size
  //    log.info(s">>>>>>>> The RDD Processor size of the serialization is $sz")
  //  }

  def main(args: Array[String]): Unit = {

    log.info("About to read the command line arguments")
    val success = args.forall(a => processArg(a))
    if (success == false) {
      log.error("One or more command line argument failed. Exiting...")
      return
    }

    //set app name to orderbook
    val appName: String = this.appNameOption.getOrElse("orderbook")

    log.info(s"main: Run appName $appName ...")
    this.configPath match {
      case Some(configPath) => {
        if (appName == "orderbook") {
          orderBookMain(appName, configPath)
        } else if (appName == "fusion") {
          fusionMain(appName, configPath)
        } else {
          val exceptionMessage = s"main: $appName: appName not recognized"
          log.error(exceptionMessage)
          throw new Exception(exceptionMessage)
        }
      }
      case None => {
        val exceptionMessage = s"main: $appName: config path is not defined"
        log.error(exceptionMessage)
        throw new Exception(exceptionMessage)
      }
    }
  }


  /** start orderbook fusion tables -> events application */
  def fusionMain(appName: String, configPath: String): Unit = {

    // set appNameOption and configPath
    if (!this.appNameOption.isDefined) {
      this.appNameOption = Some(appName)
    }

    if (!this.configPath.isDefined) {
      this.configPath = Some(configPath)
    }

    log.info(s"fusionMain: $appName: Creating Spark Session...")
    // create spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)

    sparkSession.sparkContext.setLogLevel("WARN")

    val bb = FusionEventGenerator(sparkSession, configPath)

    val cnt1 = bb.readInitialAvro(readFrom, startDate)

    val cnt2 = bb.catchUp(startDate, true) // catch up, then continue waiting for fresh

    log.info("Finished...")
  }

  def orderBookMain(appName: String, configPath: String): Unit = {

    // set appNameOption and configPath
    if (!this.appNameOption.isDefined) {
      this.appNameOption = Some(appName)
    }

    if (!this.configPath.isDefined) {
      this.configPath = Some(configPath)
    }

    // read orderbook app configuration
    readConfigurationSections(configPath)

    if (kafkaConfiguration.isDefined == false) {
      log.error("Kafka configuration not read. Exiting.")
    }

    if (hBaseConfiguration.isDefined == false) {
      log.error("HBase configuration not read. Exiting.")
      return
    }

    log.info("OrderbookMain: Hbase configuration: " + hBaseConfiguration.get.toString())

    log.info(s"OrderbookMain: Creating HDFS logger with path: ${hdfsLogConfig.get.path} and prefix: ${hdfsLogConfig.get.fileNamePrefix}")
    val errorLogger = new HdfsLogger(hdfsLogConfig.get.path, hdfsLogConfig.get.fileNamePrefix, hadoopConfiguration)

    log.info(s"orderBookMain: appName: Creating Spark Session...")
    // create spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)

    //set log level to warn
    sparkSession.sparkContext.setLogLevel("WARN")

    val sparkContext = sparkSession.sparkContext
    log.info("Creating SparkSQL Context....")
    sqlContext = sparkSession.sqlContext

    runKafkaConsumer(appName, errorLogger, sparkContext)

    log.info("Finished...")
  }

  private def runKafkaConsumer(appName: String, errorLogger: _root_.com.KanariDigital.app.AuditLog.HdfsLogger, sparkContext: SparkContext) = {
    log.info("Creating streaming context...")
    val streamingContext = StreamingContextFactory.createStreamingContext(kafkaConfiguration.get, sparkContext)
    // Check pointing  creation
    val checkpoint = kafkaConfiguration.get.streamingContextCheckpoint
    if (checkpoint.isDefined) {
      val x: String = checkpoint.get
      log.info(s"Setting context checkpoint to $x")
      streamingContext.checkpoint(x)
    } else {
      log.warn("No Streaming Context ")
    }
    log.info("Creating kafka consumer factory")
    val kafkaConsumerTry = KafkaConsumerFactory(kafkaConfiguration.get, streamingContext, errorLogger)
    log.info("Finished creating kafka consumer factory")

    kafkaConsumerTry match {

      case Success(consumer) => {
        log.info("runKafkaConsumer: Creating ConcreteHDFSWriterFactory: " + this.configPath)
        val hdfsFactory = new ConcreteHdfsWriterFactory(this.configPath)
        val context = new OrderBookContext(hBaseConfiguration.get, hdfsFactory, auditLogConfig.get, errorLogger, xform.get, obAppConfig.get)
        val processor = new KafkaStreamProcessor(appName, context)
        if (heartBeatLogger.isDefined) {
          processor.setHeartbeat(heartBeatLogger.get)
        }
        log.info("About to call run on KafkaStreamProcessor")
        processor.run(consumer)
      }

      case Failure(f) => {
        val reason = f.getMessage
        log.error(s"Unable to construct Kafka Consumer: $reason")
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
