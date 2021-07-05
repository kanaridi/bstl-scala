/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler

import java.text.SimpleDateFormat
import java.time.Instant

import com.KanariDigital.app.AppContext
import com.kanaridi.common.util.HdfsUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

case class NotebookRule(notebook: String, content: String, version: String)

case class RuleResult(content: String, ruleversion: String)

class DataWriter(ss: SparkSession, appContext: AppContext) {
  val log: Log = LogFactory.getLog(DataWriter.getClass.getName)

  val hadoopConfiguration = new HadoopConfiguration()
  private var resultsOutput = ArrayBuffer.empty[String]

  def persist(notebookId: String, contentStr: String, rulesArr: java.util.ArrayList[String], ruleContentArr: java.util.ArrayList[String]): Unit = {

    import ss.implicits._

    import scala.collection.JavaConverters

    val rules = JavaConverters.collectionAsScalaIterableConverter(rulesArr).asScala.toSeq
    val rulesContent = JavaConverters.collectionAsScalaIterableConverter(ruleContentArr).asScala.toSeq
    val timestamp: Long = Long.MaxValue - System.currentTimeMillis / 1000

    val df = Seq(NotebookRule(notebookId, contentStr, timestamp.toString)).toDF

    val keyFormat = s"masterrule$timestamp"
    val key = udf(() => keyFormat)
    var dfNew = df.withColumn("key", key())

    val rulesUdf = udf(() => rules.mkString("|"))
    dfNew = dfNew.withColumn("ruleorder", rulesUdf())

    // Write recent master row
    writeHBaseTable(dfNew, Constants.ProcessRepoMileStoneTemplate)

    // Write each rule per row
    var index = 0
    for (ruleContent <- rulesContent) {
      val ruleKeyFormat = s"${keyFormat}_${rules(index)}"
      val ruleKey = udf(() => ruleKeyFormat)
      val ruleDf = Seq(NotebookRule(notebookId, ruleContent, timestamp.toString)).toDF
      val ruleDfNew = ruleDf.withColumn("key", ruleKey())
      writeHBaseTable(ruleDfNew, Constants.ProcessRepoMileStoneTemplate)
      index += 1
    }
  }

  def insertResultObject(appendedResultObject: String): Unit = {

    // Insert object into resultOutput of the run
    resultsOutput += appendedResultObject
  }

  def flushResults(): String = {

    val readTimer = Instant.now

    // Write resultsOutput to HDFS stream
    //val kanariResultsStorageRootPath = appContext.appConfig.rulesConfigMap.getOrElse("dependency-path", Constants.KanariTestResultStorageDefaultPath)
    val kanariResultsStorageRootPath = ConfigHelper.getConfigHelper().getTestResultStoragePath()
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val filePath = kanariResultsStorageRootPath + "results_" + sdf.format(Instant.now().toEpochMilli) + ".json"
    log.info(s"Going to write to filePath: ${filePath} with ${resultsOutput.size} objects")
    HdfsUtils.saveFileContents(filePath, resultsOutput)

    // reset results and file name
    resultsOutput = ArrayBuffer.empty[String]

    log.info(s"Done flushing ${filePath}" )
    filePath
  }

  def writeResult(uberObject: String, ruleVersion: String): Unit = {

    // Get hlikey from uber object
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val hloJson = mapper.readValue(uberObject, classOf[Map[String, Any]])

    val resultKey = s"${hloJson.get("hlikey").get}_$ruleVersion"

    val key = udf(() => resultKey)

    import ss.implicits._
    val df = Seq(RuleResult(uberObject, ruleVersion)).toDF

    val dfNew = df.withColumn("key", key())

    // Write recent master row
    writeHBaseTable(dfNew, Constants.ResultsTemplate)
  }

  private def writeHBaseTable(df: DataFrame, template: String): Unit = {
    log.info(s"Write DataFrame to table using template $template")
    df.show
    log.info("DF Schema:")
    df.printSchema()

    val catalog = appContext.xform.cfg.hbaseCatalog(template)

    df.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}

object DataWriter {
  /**
    * Initialize Data writer object to save high level object results
    * @param ss spark session
    * @return DataWriter object which we would use to get and update high level object
    */
  def apply(ss: SparkSession, appContext: AppContext): DataWriter = {
    new DataWriter(ss, appContext)
  }
}
