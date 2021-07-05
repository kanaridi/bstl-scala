/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.streaming

import java.time.Instant

import com.KanariDigital.Orderbook._
import com.KanariDigital.app._
import com.KanariDigital.app.util.{LogTag, SparkUtils, ZeppelinLogUtils}
import com.kanaridi.common.util.{HdfsUtils, LogUtils}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/** Spark streaming app which periodically checking newly published rules
  * from files in hdfs, and run those rules
  */
class ZeppelinStreamingApp(appName: String) extends
    App(appName, AppType.OB_RULES_APP_TYPE) {

  override val appTypeStr = AppType.OB_RULES_APP_TYPE

  val log: Log = LogFactory.getLog(this.getClass.getName)

  val markerFileCompleted: String = "zeppelin_rules_processed_finished"
  val markerFileCompletedError: String = "zeppelin_rules_processed_finished_error"
  val markerFileStarted: String = "zeppelin_rules_processed_started"

  // Note that we will need to update KanariDigital.py as well if we change this name
  val markerFileDeploymentCompleted: String = "zeppelin_rules_deployment_finished"

  // Queue to store all updated rule path that we need to run.
  val pathQueue = mutable.Queue[String]()

  /**
   *  1. Additional config for rule-running:
   *  "rule-test": {
   *    "root-path": "abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules-test/", # Deploy script in Python will copy all rules and dependencies here per notebook.
   *    "since": "2020-04-20T00:00:00"
   *  }
   *  2. Create RulesRunner and load event
   *  3. Read from a specific location (defined in the config). In that location we will have subfolders of
   *  abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules-test/<notebookID>/<ruleRun_with_timestamp>/rules_part1.py
   *  abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules-test/<notebookID>/<ruleRun_with_timestamp>/rules_part2.py
   *  abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules-test/<notebookID>/<ruleRun_with_timestamp>/dependencies/
   *  4. The job will check if any folder has been updated.
   *  If a rule folder has been updated, store the location of this rule run in a queue (global queue so we would run all rules)
   *     - For each item in the queue, generate the rule contents by reading and concat-ing all the rule .py files. (or calling getMasterRules)
   *     - overrider the dependencies folder of this specific rule in AppConfig
   *     - Run the rule
   *     - output zip file to the same folder
   * @param isLocal - set to true to create a local spark streaming context
   * @param appContext - application context which contains [[MessageMapperFactory]]  and [[AppConfig]] objects
   */
  override def runApp(
    appContext: AppContext,
    isLocal: Boolean) = {

    // create spark context from spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession: SparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)

    val sparkContext = sparkSession.sparkContext

    // set log level
    val logLevel = appContext.appConfig.appConfigMap.getOrElse("log-level", "INFO")
    sparkContext.setLogLevel(logLevel)

    // runtime application id
    appContext.appConfig.runtimeAppId = sparkContext.applicationId
    val appConfig = appContext.appConfig

    // rule root path that we check on regularly to see if there is any new update
    val rootDir = appConfig.ruletestConfigMap("root-path")
    val durationInMs = 100 * 1000

    // start processing files from this date time string
    // if no since exists in config file the start time is current time
    val sinceStr = appConfig.ruletestConfigMap.getOrElse("since", "")
    var lastListFolderRun = if (sinceStr != "") SparkUtils.toMillis(sinceStr) else System.currentTimeMillis

    // Preparing Uber Objects
    val rulesRunner = BpRulesRunner(sparkSession, appContext)
    assert(rulesRunner.isInstanceOf[BpRulesRunner])

    ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "ABOUT TO CREATE BpRulesApply")
    val ruleApply = new BpRulesApply(sparkSession)
    ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"starting application $appName with ${appContext.appConfig.runtimeAppId}")

    //loop forever
    while (true) {
      // log start time
      val lastListFolderRunStr = SparkUtils.formatBatchTime(lastListFolderRun)
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"BATCH: $lastListFolderRun ($lastListFolderRunStr): start ...")

      // check the root path to get any new directory.
      // The folder structure we are using is <rootPath>/<notebookId>/<deployment_1>/
      // So we will go through all notebookId folders under rootPath, for each notebookId folder, get the newly updated (lastModified > startTime) folder.
      // Bring that folder in the pathQueue for processing.
      val checkingNotebookTime = Instant.now
      val notebookIdDirs = HdfsUtils.listDirectories(rootDir)
      notebookIdDirs.foreach(notebookIdDir => {
        processNotebook(notebookIdDir.getPath.toString, lastListFolderRun, appContext)
      })

      //set start new start time
      lastListFolderRun = System.currentTimeMillis

      // Process the queue
      while(pathQueue.nonEmpty) {
        ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"Queue IS NOT EMPTY, GOING THROUGH IT NOW")
        val currentRulePath = pathQueue.dequeue()
        val processingTime = Instant.now
        processRules(currentRulePath, appContext, rulesRunner, ruleApply)
      }

      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"going to sleep for $durationInMs milliseconds...")
      //go to sleep for some time
      Thread.sleep(durationInMs)
    }
  }

  private def processNotebook(notebookRulePath: String,
                              lastListFolderRun: Long,
                             appContext: AppContext): Unit = {
    ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"Processing notebook $notebookRulePath for run: $lastListFolderRun")
    try {
      val ruleDirectories = HdfsUtils.listDirectories(notebookRulePath)
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"CALLED listDirectories for each notebook folder")
      ruleDirectories.foreach(ruleDirectory => {
        val lastModified = ruleDirectory.getModificationTime
        val locatedFileDir = ruleDirectory.getPath.toString
        val isProcessed = isRulesPathProcessed(locatedFileDir)
        val isDeploymentCompleted = isRulesDeploymentCompleted(locatedFileDir)
        ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"CHECKING $locatedFileDir")
        ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"IsDirectory: ${ruleDirectory.isDirectory} | lastModified: $lastModified | " +
          s"lastListFolderRun: $lastListFolderRun | isProcessed: $isProcessed | isDeploymentCompleted: $isDeploymentCompleted")
        // Add folder to our queue to process if:
        // its a directory.
        // it has all necessary files.
        // it has not been processed yet.
        // To improve performance, we only check a list of folder since last time we checked
        if (ruleDirectory.isDirectory && isDeploymentCompleted && lastModified >= lastListFolderRun && !isProcessed) {
          pathQueue.enqueue(locatedFileDir)
          ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"ENQUEUED $locatedFileDir")
        }
      })
    } catch {
      case ex: Throwable => {
        val (exMsg, exStackTrace) = LogUtils.getExceptionDetails(ex)
        ZeppelinLogUtils.logZeppelinError(appContext.appConfig.runtimeAppId, s"application error with exception $exMsg  exceptionTrace: $exStackTrace")
      }
    }
  }

  /**
    * We consider this rule path is processed when we see marker file for started.
    * @param rulePath
    * @return
    */
  private def isRulesPathProcessed(rulePath: String): Boolean = {
    val foundMarkerFiles = HdfsUtils.listFiles(rulePath, markerFileStarted)
    !foundMarkerFiles.isEmpty
  }

  /**
    * Once KanariDigital finishes copying all the files over to the deployment folder
    * it will mark that folder as deployment completed by create a file name zeppelin_rules_deployment_finished
    * This function will check to see if the folder has that marker file or not
    * @param rulePath
    * @return
    */
  private def isRulesDeploymentCompleted(rulePath: String): Boolean = {
    val foundMarkerFiles = HdfsUtils.listFiles(rulePath, markerFileDeploymentCompleted)
    !foundMarkerFiles.isEmpty
  }

  private def processRules(rulePath: String,
                           appContext: AppContext,
                           rulesRunner: BpRulesRunner,
                           ruleApply: BpRulesApply): Unit = {
    // Generate the rule contents by concat all rules files.
    // Run job for this rules and dependencies
    // Output rules to the same rulePath location

    // Write a marker file so we know we start processing this rule folder
    val startedFilePath = s"$rulePath/$markerFileStarted"
    val finishedFilePath = s"$rulePath/$markerFileCompleted"
    val finishedFilePathError = s"$rulePath/$markerFileCompletedError"

    val nowTimeStr = SparkUtils.formatBatchTime(System.currentTimeMillis())
    HdfsUtils.saveFileContents(startedFilePath, ArrayBuffer(s"started on $nowTimeStr"))

    ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"Processing rule $rulePath")
    ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "LOAD EVENTS FROM 20100101")
    rulesRunner.loadEvents("20100101")

    ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "ABOUT TO GET UBER OBJECT DATAFRAMES")
    val orderObjDf = rulesRunner.getUberObjectsByName("OrderObject").persist
    val deliveryObjDf = rulesRunner.getUberObjectsByName("DeliveryObject").persist

    try {
      val ruleFiles = HdfsUtils.listFiles(rulePath, "*.py").reverse

      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "RULES FILES")
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, ruleFiles.mkString(","))
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "RULES CONFIG MAP")
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, appContext.appConfig.rulesConfigMap.getOrElse("files", ""))
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, appContext.appConfig.rulesConfigMap.getOrElse("dependency-path", ""))

      appContext.appConfig.setRulesConfigMapField("files", ruleFiles.mkString(","))
      appContext.appConfig.setRulesConfigMapField("dependency-path", s"$rulePath/dependencies/")
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "RULES CONFIG MAP UPDATED")
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, appContext.appConfig.rulesConfigMap.getOrElse("files", ""))
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, appContext.appConfig.rulesConfigMap.getOrElse("dependency-path", ""))

      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "LOADING CONFIG FROM: " + appContext.appConfig.configPath)
      com.KanariDigital.app.DataHandler.ConfigHelper.fromAppConfig(appContext.appConfig)

      val rules = rulesRunner.getMasterRules("run")

      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "ORDER RULE:")
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, rules.mkString("\n"))
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "SHOWING INPUT ORDER BEFORE RULES THEN RUNNING")

      orderObjDf.toJSON.take(1).foreach(print)
      val finalOrderDf = ruleApply.applyRules(orderObjDf, OrderObject(), "run", rules, JythonScriptLanguage())
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "SHOWING FINAL ORDER AFTER RULES")

      finalOrderDf.take(1).foreach(print)
      finalOrderDf.write.mode(org.apache.spark.sql.SaveMode.Overwrite).format("json").save(rulePath + "/output_order")

      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "SHOWING INPUT DELIVERY BEFORE RULES THEN RUNNING")

      deliveryObjDf.toJSON.take(1).foreach(print)
      val finalDeliveryDf = ruleApply.applyRules(deliveryObjDf, DeliveryObject(), "run", rules, JythonScriptLanguage() )
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, "SHOWING FINAL DELIVERY OUTPUT RULES")
      finalDeliveryDf.take(1).foreach(print)
      finalDeliveryDf.write.mode(org.apache.spark.sql.SaveMode.Overwrite).format("json").save(rulePath + "/output_delivery")

      // Write a marker file so we know we finished processing this rule folder already
      val nowTimeStr = SparkUtils.formatBatchTime(System.currentTimeMillis())
      HdfsUtils.saveFileContents(finishedFilePath, ArrayBuffer(s"Done $nowTimeStr"))
      ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"Done processing rule $rulePath")
    } catch {
      case ex: Throwable => {
        val (exMsg, exStackTrace) = LogUtils.getExceptionDetails(ex)
        HdfsUtils.saveFileContents(finishedFilePathError, ArrayBuffer(s"Error $nowTimeStr"))
        ZeppelinLogUtils.logZeppelinInfo(appContext.appConfig.runtimeAppId, s"Error processing rule $rulePath")
        ZeppelinLogUtils.logZeppelinError(appContext.appConfig.runtimeAppId, s"application error with exception $exMsg exceptionTrace: $exStackTrace")
      }
    }
  }
}
