/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app

import scala.util.{Failure, Success, Try}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}

import com.KanariDigital.app.AuditLog.{CompositeAuditLogger, HdfsLogger}
import com.kanaridi.dsql.FlowSpecParse
import com.kanaridi.xform.MessageMapper
import com.kanaridi.xform.{ MessageMapper, MessageMapperConfig, MessageMapperFactory}

/** contains application contontext that can be passed to the application
  * consists of [[AppConfig]], [[HadoopConfiguration]], [[HdfsAuditLogger]], [[MessageMapper]], [[FlowSpec]]
  * objects
  */
class AppContext (
  val appConfig: AppConfig,
  val xform: MessageMapper,
  val flowSpec: FlowSpecParse = FlowSpecParse("{}"),
  val hadoopCfgs: Seq[String] = Seq()

) extends Serializable {
  import AppContext._

  def hadoopConfiguration(): HadoopConfiguration = { hadoopCfg(hadoopCfgs) }

  // get audit log configuration
  val auditLogPath = appConfig.auditLogConfigMap("path")
  val auditLogFilenamePrefix = appConfig.auditLogConfigMap("filename-prefix")
  val hdfsAuditLogger = new HdfsLogger(auditLogPath, auditLogFilenamePrefix, hadoopConfiguration)

}

object AppContext {

  private def hadoopCfg(cfgs: Seq[String]): HadoopConfiguration = {
    val cfg = new HadoopConfiguration
    cfgs.foreach(xml =>  cfg.addResource(xml))
    cfg.set("defaultfs", "hdfs:///")
    cfg
  }

  def apply(configPath: String,
    appNameOption: Option[String],
    hadoopCfgs: Seq[String] = Seq()): AppContext = {

    val hCfg = hadoopCfg(hadoopCfgs)
    val appConfigTry = Try(AppConfig.readPipelineorAppConfig(configPath,
      appNameOption,
      hCfg))

    appConfigTry match {
      case Success(appConfig) => {
        val messageMapperPath = appConfig.messageMapperConfigMap.getOrElse("path", "")
        // check if path to mapping file has been set
        val mmCfg = if (messageMapperPath != "") {
          val fileSystem = FileSystem.get(hCfg)
          val mmcStream = fileSystem.open(new Path(messageMapperPath))
          MessageMapperConfig(mmcStream)
        } else {
          // mapping is inline
          val content = appConfig.messageMapperConfigMap.get("mapperObj")
          if ( content.isDefined) {
            MessageMapperConfig(content.get)
          } else {
            //no op mapper
            MessageMapperConfig("{}")
          }
        }

        val xform =  MessageMapperFactory(mmCfg)

        val flowSpec = {
          val fileSystem = FileSystem.get(hCfg)
          val mmcStream = fileSystem.open(new Path(configPath))
          FlowSpecParse(mmcStream)
        }

        new AppContext(appConfig, xform, flowSpec, hadoopCfgs)
      }
      case Failure(f) => {
        throw new Exception(f.getMessage)
      }
    }
  }
}

