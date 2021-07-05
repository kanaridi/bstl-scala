/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app

import scala.util.{Failure, Success, Try}

import com.KanariDigital.Orderbook.{Main => OrderbookApp}
import com.KanariDigital.app.streaming.{AbfsStreamingApp, EventhubStreamingApp, FlowApp,
  HdfsStreamingApp, KafkaStreamingApp, ZeppelinStreamingApp}
import com.KanariDigital.app.util.LogTag
import com.kanaridi.common.util.LogUtils

import org.apache.commons.logging.{Log, LogFactory}

/** Application Main
  *
  * required arguments:
  *  - config - path to the configuration file
  *  - appName - name of the application to run
  */
object AppMain {

  val log: Log = LogFactory.getLog(AppMain.getClass.getName)

  var appNameOption: Option[String] = None
  var flowNameOption: Option[String] = None
  // val hadoopConfiguration = new HadoopConfiguration()
  var configPath : Option[String] = None
  var isLocal = false

  var hadoopCfgs: List[String] = List()

  /** Contains regular expressions to parse command line options */
  object CommandLineArguments {

    val appNameRegEx = "-appName=(.*)".r
    val hdfsConfigXmlRegEx = "-resource=(.*)".r
    val configRegEx = "-config=(.*)".r
    val isLocalFlag = "-local"
    val flowNameRegEx = "-flowOp=(.*)".r

  }

  /** parse commandline arguments using regular expressions
    * specified in [[CommandLineArguments]]
    *
    *  {{-appName}} argument should correspond to section
    *               in the configuration file.
    *    * {{app-type}} property in the configuration file
    *      specifies whether to run 'eventhub_streaming" or
    *      'hdfs_streaming" application
    */
  def processArg(arg: String): Boolean = {

    log.info(s"arg = $arg")

    arg match {

      case CommandLineArguments.appNameRegEx(appStr) => {
        appNameOption = Some(appStr)
        true
      }

      case CommandLineArguments.flowNameRegEx(flowStr) => {
        flowNameOption = Some(flowStr)
        true
      }

      case CommandLineArguments.hdfsConfigXmlRegEx(xml) => {
        // hadoopConfiguration.addResource(xml)
        hadoopCfgs =  xml :: hadoopCfgs
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

  /**
    */
  def main(args: Array[String]): Unit = {

    log.debug("About to read the command line arguments")
    val success = args.forall(a => processArg(a))
    if (success == false) {
      log.error("One or more command line argument failed. Exiting...")
      return
    }

    this.configPath match {
      case Some(cfgPath) => {
        val appContextTry = Try(AppContext(cfgPath, this.appNameOption, this.hadoopCfgs))

        appContextTry match {
          case Success(appContext) => {
            val appConfig = appContext.appConfig
            val appName = appConfig.appName
            val appType = appConfig.appType

            val appOpt: Option[App] = appType match {
              case AppType.EVENTHUB_STREAMING_APP_TYPE => {
                Some(new EventhubStreamingApp(appName))
              }
              case AppType.HDFS_STREAMING_APP_TYPE => {
                Some(new HdfsStreamingApp(appName))
              }
              case AppType.ABFS_STREAMING_APP_TYPE => {
                Some(new AbfsStreamingApp(appName))
              }
              case AppType.KAFKA_STREAMING_APP_TYPE => {
                Some(new KafkaStreamingApp(appName))
              }
              case AppType.OB_FUSION_APP_TYPE => {
                Some(new KafkaStreamingApp(appName))
              }
              case AppType.OB_RULES_APP_TYPE => {
                Some(new ZeppelinStreamingApp(appName))
              }
              case AppType.FLOW_APP_TYPE => {
                Some(new FlowApp(appName))
              }
              case _ => {
                None
              }
            }

            appOpt match {
              case Some(app) => {
                app.runApp(appContext, isLocal)
              }
              case None => {

                val exMesg = LogUtils.convToJsonString(s"$appName: app-type is not defined in $configPath bailing out")

                log.error(s"""${LogTag.APPLICATION_ID_NONE} - ${LogTag.FATAL_CONFIGPATH_UNDEFINED_ERROR} -
              |appName: $appName,
              |exceptionMessage: $exMesg, exceptionTrace: $exMesg,
              |message: app type is not defined $exMesg"""
                  .stripMargin.replaceAll("\n", " "))

                throw new Exception(exMesg)
              }
            }
          }
          case Failure(f) => {

            f.printStackTrace()

            // default to legacy orderbook kafka streaming application
            val appName = "orderbook"
            log.info(s"main: Run $appName ...")
            this.configPath match {
              case Some(configPath) => {
                OrderbookApp.orderBookMain(appName, configPath)
              }
              case None => {
                log.error(s"""${LogTag.APPLICATION_ID_NONE} - ${LogTag.FATAL_CONFIGPATH_UNDEFINED_ERROR} -
                  |appName: $appName,
                  |exceptionMessage: config file is undefined, exceptionTrace: config file is undefined,
                  |message: config file is undefined"""
                  .stripMargin.replaceAll("\n", " "))
                val exceptionMessage = "$appName - config path is not defined"
                throw new Exception(exceptionMessage)
              }
            }
          }
        }
      }
      case None => {
        log.error(s"""${LogTag.APPLICATION_ID_NONE} - ${LogTag.FATAL_CONFIGPATH_UNDEFINED_ERROR} -
                  |exceptionMessage: config file is undefined, exceptionTrace: config file is undefined,
                  |message: config file is undefined"""
            .stripMargin.replaceAll("\n", " "))
          val exceptionMessage = s"config path is undefined"
        throw new Exception(exceptionMessage)
      }
    }
  }
}
