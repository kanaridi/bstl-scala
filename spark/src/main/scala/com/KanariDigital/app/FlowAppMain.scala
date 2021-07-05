/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app

import com.KanariDigital.app.util.SparkUtils
import com.KanariDigital.app.util.LogTag
import com.kanaridi.common.util.HdfsUtils
import com.kanaridi.dsql.FlowSpecParse

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object FlowAppMain {
  val log: Log = LogFactory.getLog(this.getClass.getName)

  var appNameOption: Option[String] = None
  var signalIdOption: Option[String] = None
  var configPath : Option[String] = None
  var isLocal = false

  /** Contains regular expressions to parse command line options */
  object CommandLineArguments {
    val appNameRegEx = "-appName=(.*)".r
    val configRegEx = "-config=(.*)".r
    val isLocalFlag = "-local"
    val signalIdRegEx = "-signalId=(.*)".r
  }

  def processArg(arg: String): Boolean = {
    log.info(s"arg = $arg")

    arg match {
      case CommandLineArguments.appNameRegEx(appStr) => {
        appNameOption = Some(appStr)
        true
      }
      case CommandLineArguments.signalIdRegEx(signalStr) => {
        signalIdOption = Some(signalStr)
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
        log.error(s"Unable process argument $arg. Ignoring.....")
        true
      }
    }
  }

  /**
    * Starts a SparkSession, sets com.kanaridi.rest.Service.spark,
    * Then waits around
    * @param args
    */
  def main(args: Array[String]): Unit = {
    log.info("About to read the command line arguments")
    val success = args.forall(a => processArg(a))
    if (success == false) {
      log.error("One or more command line argument failed. Exiting...")
      return
    }

    val appName = appNameOption.getOrElse("FlowAppMain")

    log.info(s"$appName: start")

    // create spark context from spark conf
    val sparkConf = new SparkConf()
      .setAppName(s"$appName")
      .set("spark.serializer", classOf[KryoSerializer].getName)
    val sparkSession: SparkSession = SparkUtils.createSparkSession(appName, sparkConf, isLocal)

    com.kanaridi.rest.Service.spark = sparkSession

    // set log level
    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("WARN")

    // runtime application id
    val runtimeApplicationId = sparkContext.applicationId

    log.info(s"""$runtimeApplicationId - ${LogTag.START_APP} -
                |appName: $appName, appType: ${AppType.FLOW_APP_TYPE},
                |message: starting application $appName with $runtimeApplicationId""".stripMargin.replaceAll("\n", " "))

    this.configPath match {
      case Some(cfgPath) => {
        val fileSystem = FileSystem.get(new HadoopConfiguration())
        val flowSource = fileSystem.open(new Path(cfgPath))
        val mmcfg = FlowSpecParse(flowSource)
        val sigId = signalIdOption.getOrElse("out")

        val pipelineId = mmcfg.getPipelineId

        val checkpointDir = mmcfg.getCheckpointDir
        // get app log dir, if path is relative construct path in users
        // home directory
        val appLogPath = HdfsUtils.getAppDir(checkpointDir, pipelineId)
        if (appLogPath.isDefined) {
          val dirName = appLogPath.get.toUri.getPath
          log.info(s"creating checkpoint directory: $dirName")
          HdfsUtils.createDirectory(dirName)
          // set checkpoint directory
          sparkContext.setCheckpointDir(dirName)
        }

        val flowOpt = mmcfg.getDownstreamOp(sigId, sparkSession)
        flowOpt match {
          case Some(flow) => flowOpt.get.run()
          case None => {
            log.error(s"no signal named $sigId found in $cfgPath. Aborting")
          }
        }
      }
      case None => {
        // get app log dir, if path is relative construct path in users
        // home directory
        val appLogPath = HdfsUtils.getAppDir("flowapi", "")
        if (appLogPath.isDefined) {
          val dirName = appLogPath.get.toUri.getPath
          log.info(s"creating checkpoint directory: $dirName")
          HdfsUtils.createDirectory(dirName)
          // set checkpoint directory
          sparkContext.setCheckpointDir(dirName)
        }
        //loop forever, look for input on API port
        while(true) {
          val durationInMs = 100 * 1000
          log.info(s"Sleeping for $durationInMs ms")
          Thread.sleep(durationInMs)

        }
      }
    }
  }
}

