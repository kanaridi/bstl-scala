/* Copyright (c) 2020 Kanari Digital, Inc. */


package com.KanariDigital.app.streaming

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import com.KanariDigital.app.{App, AppContext, AppType, RddProcessor, RddProcessorOpts}
import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, UnitOfWork}
import com.KanariDigital.app.util.{LogTag, SparkUtils}
import com.kanaridi.common.util.{HdfsUtils, LogUtils}

/** HDFS spark streaming app which periodically retrieves data
  * from files in hdfs, transforms and writes the transformed data to a
  * series of hbase tables
  */
class HdfsStreamingApp(appName: String) extends
    App(appName, AppType.HDFS_STREAMING_APP_TYPE) {

  override val appTypeStr = AppType.HDFS_STREAMING_APP_TYPE

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** run hdfs streaming application which processes raw data from hdfs
    * @param appName - application name
    * @param isLocal - set to true to create a local spark streaming context
    * @param appContext - application context which contains,
    *                     [[MessageMapperFactory]]  and [[AppConfig]] objects
    */
  override def runApp(
    appContext: AppContext,
    isLocal: Boolean): Unit = {

    log.info(s"$appName: runApp: start")

    // create spark context from spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession: SparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)

    val sparkContext = sparkSession.sparkContext
    // set log level
    val logLevel = appContext.appConfig.appConfigMap.getOrElse("log-level", "WARN")
    sparkContext.setLogLevel(logLevel)

    val sqlContext = sparkSession.sqlContext

    // runtime application id
    val runtimeApplicationId = sparkContext.applicationId
    appContext.appConfig.runtimeAppId = runtimeApplicationId

    val appConfig = appContext.appConfig

    // application id
    val applicationId = appConfig.getApplicationId

    // hdfs streaming app
    val incomingDirs: String = appConfig.hdfsStreamingConfigMap("incoming")

    // hdfs scratch dir
    val scratchDir = appConfig.hdfsStreamingConfigMap.getOrElse("scratch-dir", "")

    // merge files
    val mergeSourceFiles = appConfig.hdfsStreamingConfigMap.getOrElse("merge-source-files", "false").toBoolean

    // move files to archive
    val archiveDir = appConfig.hdfsStreamingConfigMap.getOrElse("archive", "")

    val maxFilesBatch:Int = appConfig.hdfsStreamingConfigMap.getOrElse("files-batch-size", "1").toInt

    // process files that have last modified timestamp than slack time
    val modifiedAgoSeconds: Long = appConfig.hdfsStreamingConfigMap.getOrElse("files-modified-ago", "600").toLong
    val modifiedAgo = modifiedAgoSeconds * 1000

    // bstl expression to select sort field in incoming records
    //$.header.timestamp FIXME: remove?
    val sortDataBy: String = appConfig.hdfsStreamingConfigMap.getOrElse("files-sort-data-by", "")

    // max partitions allowed
    val maxFilesPartitions: Int = appConfig.hdfsStreamingConfigMap.getOrElse("files-max-partitions", "180").toInt

    // topic - FIXME remove?
    val hdfsTopics = appConfig.hdfsStreamingConfigMap("topics")
    val hdfsTopicJsonPath = appConfig.hdfsStreamingConfigMap.getOrElse("topic-json-path", "")

    // format and schema of the files to ingest
    val filesFormat = appConfig.hdfsStreamingConfigMap.getOrElse("files-format", "json").toLowerCase

    // get reading options for format type
    val filesOptions: Map[String, String] = if (filesFormat == "csv") {
      Map(
        "header" -> appConfig.hdfsStreamingConfigMap.getOrElse("files-format-csv-header", "true"),
        "sep" -> appConfig.hdfsStreamingConfigMap.getOrElse("files-format-csv-separator", ","),
        "inferSchema" -> appConfig.hdfsStreamingConfigMap.getOrElse("files-format-csv-inferschema", "false")
      )
    } else if (filesFormat == "parquet") {
      Map(
        "mergeSchema" -> appConfig.hdfsStreamingConfigMap.getOrElse("files-format-parquet-mergeschema", "true")
      )
    } else {
      //default options
      Map.empty[String, String]
    }

    // schema
    val filesSourceSchema: String = if (filesFormat == "csv") {
      appConfig.hdfsStreamingConfigMap.getOrElse("files-format-csv-schema", "")
    } else {
      ""
    }

    // duration
    val durationSeconds = appConfig.sparkConfigMap("batchDuration").toLong
    val duration = durationSeconds * 1000

    // start processing files from this date time string
    // if no since exists in config file the start time is current time
    val startTimeStr: String = appConfig.hdfsStreamingConfigMap.getOrElse("since", "")

    // source file pattern
    val filenameRegex = appConfig.hdfsStreamingConfigMap.getOrElse("filename-regex", ".+")
    val directoryRegex = appConfig.hdfsStreamingConfigMap.getOrElse("directory-regex", ".+")

    log.warn(s"""$runtimeApplicationId - ${LogTag.START_APP} -
                 |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                 |message: starting application $applicationId $appName with $runtimeApplicationId""".stripMargin.replaceAll("\n", " "))

    iterateDirs(appContext, sparkContext, incomingDirs, scratchDir, mergeSourceFiles,
      archiveDir, maxFilesBatch, modifiedAgo,
      sortDataBy, maxFilesPartitions: Int, hdfsTopicJsonPath, filesFormat,
      filesOptions, filesSourceSchema, duration, startTimeStr,
      filenameRegex, directoryRegex)

  }

  /** iterate directories and process files */
  protected def iterateDirs(appContext: AppContext, sparkContext: SparkContext, incomingDirs: String, scratchDir: String,
    mergeSourceFiles: Boolean, archiveDir: String, maxFilesBatch: Int, modifiedAgo: Long,
    sortDataBy: String, maxFilesPartitions: Int, hdfsTopicJsonPath: String, filesFormat: String,
    filesOptions: Map[String, String], filesSourceSchema: String, duration: Long, startTimeStr: String,
    filenameRegex: String, directoryRegex: String) = {

    var startTime = if (startTimeStr != "") SparkUtils.toMillis(startTimeStr) else System.currentTimeMillis

    // application id
    val applicationId = appContext.appConfig.getApplicationId

    //loop forever
    while (true) {

      var currentBatch = ArrayBuffer.empty[LocatedFileStatus]

      // new iteration
      var now = System.currentTimeMillis()
      var nextTime: Long = now - 3600000    // one hour ago?

      //var batchTime = now

      // batch start
      var startBatch = System.currentTimeMillis
      val runtimeApplicationId = appContext.appConfig.runtimeAppId

      // batchTime str
      var batchTimeMillisecs: Long = now
      var batchTimeStr = SparkUtils.formatBatchTime(batchTimeMillisecs)


      log.info(s"BATCH: $batchTimeMillisecs ($batchTimeStr): start ...")

      // log total counts
      var sourceAllTotal: Long = 0

      incomingDirs.split(",").foreach( incomingDir => {

        log.info(s"BATCH: $now: ("
          + SparkUtils.formatBatchTime(now) + "): getting list of files from {"
          + incomingDir + "} with last modified time > {"
          + SparkUtils.formatBatchTime(startTime) + "}...")

        try {

          // get an iterator and start processing files in depth first order
          val filesIterOption = HdfsUtils.getDirectoryIterator(incomingDir)

          if (!filesIterOption.isDefined) {

            log.warn(s"Cannot get iterator $incomingDir, ignoring ...")

          } else {

            val filesIter: RemoteIterator[LocatedFileStatus] = filesIterOption.get

            while (filesIter.hasNext) {

              val locatedFileStatus = filesIter.next()

              val lastModified = locatedFileStatus.getModificationTime
              val locatedFileDir = locatedFileStatus.getPath.getParent.toString
              val locatedFilename = locatedFileStatus.getPath.getName.toString

              // get files with last modified greated than start time,
              // that are not zero bytes, and were modified earlier
              if (locatedFileStatus.isFile &&
                (lastModified > startTime && (now - lastModified > modifiedAgo)) &&
                locatedFileStatus.getLen > 0) {

                // directory where directory regex has a match
                val dirMatches = directoryRegex.r.findFirstIn(locatedFileDir)
                dirMatches match {
                  case Some(x) => {
                    // log.debug(s"directoryRegex $directoryRegex matches $locatedFileDir")
                    //file where file regex has a match
                    val filenameMatches = filenameRegex.r.findFirstIn(locatedFilename)
                    filenameMatches match {
                      case Some(x) => {
                        // log.debug(s"filenameRegex $filenameRegex matches $locatedFilename")
                        currentBatch += locatedFileStatus
                      }
                      case None => {
                        //ignore, filename doesnt match regex
                      }
                    }
                  }
                  case None => {
                    // ignore, directory doesnt match regex
                  }
                }
              }

              if (currentBatch.size >= maxFilesBatch) {

                val filesCount = currentBatch.size

                log.warn(s"""$runtimeApplicationId - ${LogTag.SOURCE_TOPIC_COUNT} -
                  |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                  |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
                  |count: $filesCount,
                  |message: found $filesCount files"""
                  .stripMargin.replaceAll("\n", " "))

                sourceAllTotal = sourceAllTotal + filesCount.toLong

                processFiles(
                  sparkContext,
                  appContext,
                  hdfsTopicJsonPath,
                  now,
                  currentBatch, sortDataBy, maxFilesPartitions, archiveDir,
                  scratchDir, mergeSourceFiles,
                  filesFormat, filesSourceSchema, filesOptions,
                  runtimeApplicationId, batchTimeMillisecs)

                //reset current batch
                currentBatch = ArrayBuffer.empty[LocatedFileStatus]

              }

            } // files iterator

            //process any files leftover from currentBatch
            if (currentBatch.size > 0) {

              val filesCount = currentBatch.size

              log.warn(s"""$runtimeApplicationId - ${LogTag.SOURCE_TOPIC_COUNT} -
                  |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
                  |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
                  |count: $filesCount,
                  |message: found $filesCount files == $currentBatch"""
                .stripMargin.replaceAll("\n", " "))

              sourceAllTotal = sourceAllTotal + filesCount.toLong

              processFiles(
                sparkContext,
                appContext,
                hdfsTopicJsonPath,
                now,
                currentBatch, sortDataBy, maxFilesPartitions, archiveDir,
                scratchDir, mergeSourceFiles,
                filesFormat, filesSourceSchema, filesOptions,
                runtimeApplicationId, batchTimeMillisecs)

              //reset current batch
              currentBatch = ArrayBuffer.empty[LocatedFileStatus]
            }
          }
        } catch {

          case ex: Throwable => {

            val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

            log.error(s"""$runtimeApplicationId - ${LogTag.APP_ERROR} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
              |message: application error with exception $exMesg"""
              .stripMargin.replaceAll("\n", " "))

            // dont change the next time
            nextTime = startTime
          }
        }

        log.info(s"BATCH: $now: ("
          + SparkUtils.formatBatchTime(now) + "): done processing files from {"
          + incomingDir + "} with last modified time > {"
          + SparkUtils.formatBatchTime(startTime) + "}.")

      })

      val durationBatch = System.currentTimeMillis - startBatch
      log.warn(s"""$runtimeApplicationId - ${LogTag.BATCH_DURATION} -
              |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
              |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
              |duration: $durationBatch, count: $sourceAllTotal,
              |message: batch processed $sourceAllTotal files in $durationBatch milliseconds"""
              .stripMargin.replaceAll("\n", " "))

      log.info(s"going to sleep for $duration milliseconds...")

      //go to sleep for some time
      Thread.sleep(duration)

      //set start new start time
      startTime = nextTime
    }
  }


  /** get RDD from files in a batch.
    * If the current batch has more than one files do a union
    * of all the RDD's if the batch has only one file just convert
    * the single file to a RDD
    */
  private def filesToRDD(
    sparkContext: SparkContext,
    filesFormat: String,
    filesSourceSchema: String,
    filesOptions: Map[String, String],
    currentBatchFiles: ArrayBuffer[LocatedFileStatus]): RDD[String] = {

    if (currentBatchFiles.size == 1) {
      // we are processing one file at a time
      val filePath = currentBatchFiles(0).getPath.toString
      filesFormat.toLowerCase match {
        case "csv"  => {
          val sparkSession = SparkSession.builder().getOrCreate()
          val rawDF = if ( filesSourceSchema != "" ) {
            val customSchema = StructType(filesSourceSchema.split(",").map(fieldName => StructField(fieldName.trim, StringType, true)))
            sparkSession.read.format("csv").options(filesOptions).schema(customSchema).load(filePath)
          } else{
            sparkSession.read.format("csv").options(filesOptions).load(filePath)
          }
          rawDF.toJSON.rdd
        }
        case "avro"  => {
          val sparkSession = SparkSession.builder().getOrCreate()
          val rawDF = sparkSession.read.format("avro").options(filesOptions).load(filePath)
          rawDF.toJSON.rdd
        }
        case "parquet"  => {
          val sparkSession = SparkSession.builder().getOrCreate()
          val rawDF = sparkSession.read.format("parquet").options(filesOptions).load(filePath)
          rawDF.toJSON.rdd
        }
        case "orc"  => {
          val sparkSession = SparkSession.builder().getOrCreate()
          val rawDF = sparkSession.read.format("orc").options(filesOptions).load(filePath)
          rawDF.toJSON.rdd
        }
        case _  => {
          sparkContext.textFile(filePath)
        }
      }
    } else {
      // construct a filepath list
      val filePaths = currentBatchFiles.map(oneFile => {
        oneFile.getPath.toString()
      })

      filesFormat match {
        case "csv"  => {
          val sparkSession = SparkSession.builder().getOrCreate()
          val rawDF = sparkSession.read.format("csv").options(filesOptions).load(filePaths: _*)
          rawDF.toJSON.rdd
        }
        case "avro"  => {
          val sparkSession = SparkSession.builder().getOrCreate()
          val rawDF = sparkSession.read.format("avro").options(filesOptions).load(filePaths: _*)
          rawDF.toJSON.rdd
        }
        case "parquet"  => {
          val sparkSession = SparkSession.builder().getOrCreate()
          val rawDF = sparkSession.read.format("parquet").options(filesOptions).load(filePaths: _*)
          rawDF.toJSON.rdd
        }
        case "orc"  => {
          val sparkSession = SparkSession.builder().getOrCreate()
          val rawDF = sparkSession.read.format("orc").options(filesOptions).load(filePaths: _*)
          rawDF.toJSON.rdd
        }
        case _  => {
          // create a RDD
          // from a comma-separated filepath's
          sparkContext.textFile(filePaths.mkString(","))
        }
      }
    }
  }

  /** get RDD from files in a batch.
    * If the current batch has more than one files concat the files together
    * RDD's if the batch has only one file just convert
    * the single file to a RDD
    */
  private def filesConcatToRDD(
    sparkContext: SparkContext,
    destinationFile: Path,
    currentBatchFiles: ArrayBuffer[LocatedFileStatus]): RDD[String] = {

    if (currentBatchFiles.size == 1) {

      // we are processing one file at a time
      val filePath = currentBatchFiles(0).getPath.toString
      log.info(s"sourceFile: $filePath")
      sparkContext.textFile(filePath)

    } else {

      val start = System.currentTimeMillis()

      // construct a filepath list
      val filePaths = currentBatchFiles.map(oneFile => {
        log.info(s"sourceFile: ${oneFile.getPath.toString}")
        oneFile.getPath
      })

      // concat files in hdfs
      HdfsUtils.concatFiles(destinationFile, filePaths)

      val end = System.currentTimeMillis()
      val duration = end - start
      log.info(s"concatanated ${currentBatchFiles.size} files to ${destinationFile} in $duration milliseconds")

      // create a RDD from concatanated file
      sparkContext.textFile(destinationFile.toString)
    }
  }

  /** process all files in a batch */
  private def processFiles(
    sparkContext: SparkContext,
    appContext: AppContext,
    hdfsTopicJsonPath: String,
    currentBatchMillis: Long,
    currentBatchFiles: ArrayBuffer[LocatedFileStatus],
    sortDataBy: String,
    maxFilesPartitions: Int,
    archiveDir: String,
    scratchDir: String,
    mergeSourceFiles: Boolean,
    fileFormat: String,
    fileSourceSchema: String,
    fileOptions: Map[String, String],
    runtimeApplicationId: String,
    batchTime: Long): Unit = {

    var destinationFile: Option[Path] = None

    try {

      val start = System.currentTimeMillis()

      destinationFile = HdfsUtils.getTemporaryFile(scratchDir, appName, currentBatchMillis)

      val filesRDD: RDD[String] = if (mergeSourceFiles) filesConcatToRDD(sparkContext, destinationFile.get, currentBatchFiles) else filesToRDD(
        sparkContext, fileFormat, fileSourceSchema, fileOptions, currentBatchFiles)

      val coalescedRDD = if (filesRDD.getNumPartitions > maxFilesPartitions) filesRDD.coalesce(maxFilesPartitions) else filesRDD

      //get topic content rdd
      val topicContentRDD: RDD[(String, String)] = SparkUtils.getTopicContentRDD(hdfsTopicJsonPath, coalescedRDD, batchTime, appContext)

      //sort the rdd if necessary
      val sortedRDD = if (sortDataBy.length > 0) SparkUtils.sortDataDF(topicContentRDD, sortDataBy, batchTime, appContext) else topicContentRDD
      // sortedRDD.collect().foreach(rdd => log.info("SORTED REC: " + rdd))

      val defaultStorageLevelStr = appContext.appConfig.appConfigMap.getOrElse(
        "default-storage-level",
        "")
      val defaultStorageLevel = SparkUtils.getStorageLevel(defaultStorageLevelStr)
      sortedRDD.persist(defaultStorageLevel)

      var encounteredException : Option[Exception] = None

      val auditLog = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)

      // process and transform data
      val opts: Set[RddProcessorOpts] = Set(
            RddProcessorOpts.REPART_SORT)
      val rddProcessor = new RddProcessor(appContext, appName, opts)

      log.debug("currentBatchFiles: " + currentBatchFiles)
      sortedRDD.take(10).foreach(println)

      //set unit of work
      val uow = UnitOfWork.createUnitOfWork()
      encounteredException = rddProcessor.processRDD(uow, sortedRDD, auditLog)

      // if (encounteredException.isDefined == false) {
      //   //wite the raw records to hdfs
      //   encounteredException = storeRawData(appContext, appName, topicAndJson)
      // }

      if (encounteredException.isDefined) {
        appContext.hdfsAuditLogger.logException(encounteredException.get)
        throw new Exception(encounteredException.get)
      } else {

        //since processing was sucessful move files in this batch to archive
        val logBatchTimeStr = SparkUtils.formatBatchTime(currentBatchMillis)

        if (archiveDir.length > 0) {
          val currentBatchFilePaths = currentBatchFiles.map(locatedFileStatus => {
            locatedFileStatus.getPath.toString
          })
          HdfsUtils.moveFilesPreservePath(currentBatchFilePaths.toArray, archiveDir)
        }

        log.info(s"$appName: start writing processed files to audit log...")

        // going to log the offsets
        val auditLogger = CompositeAuditLogger.createLogAuditLogger().withLogger(appContext.hdfsAuditLogger)
        currentBatchFiles.foreach(oneFile => {
          val logJson = fileToJSONString(appName, currentBatchMillis, oneFile.getPath.toString)
          auditLogger.insertLogMessage(s"$uow: " + logJson)
        })
        auditLogger.flush()
        log.info(s"$appName: done writing processed files to audit log.")
      }

      sortedRDD.unpersist()

      val end = System.currentTimeMillis()

      val firstFile = currentBatchFiles.head.getPath
      val lastFile = currentBatchFiles.last.getPath
      val batchSize = currentBatchFiles.size
      val duration = end - start
      log.info(s"processed batch:  $firstFile to $lastFile...")
      log.info(s"processed $batchSize files in $duration milliseconds")

    } finally {
      // remove temp file if it exists
      if (destinationFile.isDefined) HdfsUtils.removeFile(destinationFile.get, scratchDir)
    }
  }

  /** create a json string of files that were processed for the audit logs */
  private def fileToJSONString(appName: String, batchTime: Long, file: String): String = {

    val offsetMap = Map(
      "appName" -> appName,
      "batchTime" -> batchTime,
      "batchTimeStr" -> SparkUtils.formatBatchTime(batchTime),
      "files" -> file)

    //parse map as json
    val jsonCellsMapper = new ObjectMapper()
    jsonCellsMapper.registerModule(DefaultScalaModule)
    val offsetRangeJson = jsonCellsMapper.writeValueAsString(offsetMap)
    offsetRangeJson
  }

}
