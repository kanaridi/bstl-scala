/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.util

import java.text.SimpleDateFormat
import java.time.Instant

import scala.collection.immutable.{Map => ImmutableMap}

import com.KanariDigital.app.AppContext

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SQLContext, SaveMode, SparkSession}

import org.apache.spark.sql.SaveMode

import com.kanaridi.xform.HBRow
import com.kanaridi.common.util.{HdfsUtils, LogUtils}

/** utils for storing raw data in hdfs
  */
object StoreRawUtils {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** store contents of source RDD in HDFS
    *
    * {{
    *   $STORE_RAW_PATH/yearmonth={$YYYY}{$MM}/daytimestamp={$dd}{$HH}{$mm}{$ss}/part-*.txt
    * }}
    *
    *  e.g.
    *
    * {{
    *  /HDFS_ROOT/EA/supplychain/raw/ob/ORDERS/yearmonth=201907/daytimestamp=17205959/part-00000-63b71f8a-be42-4a3f-8358-0715a8addea3.txt
    * }}
    *  uberObjDf.write.mode(SaveMode.Append).format("com.databricks.spark.avro").save("abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/inprogress/order_uber/raw")
    *  uberObjDf.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save("abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/inprogress/order_uber/raw")
    *  uberObjDf.repartition(1).write.mode(SaveMode.Append).format("com.databricks.spark.avro").save("abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/inprogress/order_uber/raw")
    *  uberObjDf.coalesce(1).write.format("com.databricks.spark.avro").save("/user/cloudbreak/testavrowrite2")
    *  uberObjDf.repartition(50, $"hliserial").write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").partitionBy(""),save("abfs://prodimport@kanaridataexchange.dfs.core.windows.net/EA/supplychain/inprogress/order_uber/raw")
    */
  def storeRawData(
    appContext: AppContext,
    batchTimeMillisecs: Long,
    topics: Array[String],
    sqlContext: SQLContext,
    storeRawEnabled: String,
    storeRawPath:String,
    storeRawFormat: String,
    storeRawMapping: ImmutableMap[String, String],
    rdd: RDD[(String, String)]): Option[Exception] = {

    val appName = appContext.appConfig.appName
    val applicationId = appContext.appConfig.appId
    val appTypeStr = appContext.appConfig.appType
    val runtimeApplicationId = appContext.appConfig.runtimeAppId
    val batchTimeStr = SparkUtils.formatBatchTime(batchTimeMillisecs)

    try {

      if (storeRawEnabled == "true") {

        log.info(s"$appName storeRawEnabled is true: Writing Raw JSON to HDFS...")
        topics.foreach(topic => {
          log.info(s"$appName Writing Raw JSON to HDFS for topic: {$topic}...")
          writeRawRecords(
            appName, sqlContext,
            storeRawFormat, storeRawPath,
            storeRawMapping, topic,
            rdd.filter(pr => pr._1 == topic).map(pr => pr._2))
          log.info(s"$appName Writing Raw JSON to HDFS for topic: {$topic} done.")
        })
      } else {
        log.info(s"$appName storeRawEnabled is false: Skipping Raw JSON writing.")
      }

    } catch {

      case ex: Exception => {

        val (exMesg, exStackTrace) = LogUtils.getExceptionDetails(ex)

        log.error(s"""$runtimeApplicationId - ${LogTag.APP_ERROR} -
          |applicationId: $applicationId, appName: $appName, appType: $appTypeStr,
          |batchTime: $batchTimeMillisecs, batchTimeStr: $batchTimeStr,
          |exceptionMessage: $exMesg, exceptionTrace: $exStackTrace,
          |message: error storing raw data to hdfs $exMesg"""
          .stripMargin.replaceAll("\n", " "))

        return Some(ex)
      }
    }

    //success
    return None
  }

  /** Write the raw data read from eventhub to hdfs */
  def writeRawRecords(
    appName: String,
    sqlContext:SQLContext,
    storeRawFormat: String,
    storeRawPath: String,
    storeRawMapping: ImmutableMap[String, String],
    topic: String,
    topicRecords: RDD[String]): Unit = {

    val path = getRawHdfsFilePath(topic, storeRawPath, storeRawMapping)

    val rddC = topicRecords.coalesce(1)

    log.warn(s"StoreRawUtils: $appName: writeRawRecords: writing topic '$topic' to path '$path' format '$storeRawFormat'")

    val coalesced = convertRDDToDF(sqlContext, rddC).coalesce(1)

    //dont compress
    val optionsMap: Map[String, String] = Map("compression" -> "none")

    storeRawFormat match {

      case "txt" => {
        coalesced.write.mode("append").options(optionsMap).text(path)
      }
      case "text" => {
        coalesced.write.mode("append").options(optionsMap).text(path)
      }
      case "json" => {
        coalesced.write.mode("append").json(path)
      }

      case "avro" =>  {
        coalesced.write.mode(SaveMode.Append).format("com.databricks.spark.avro").save(path)
      }

      case _ =>  {
        throw new Exception(s"Unknown save-mode $storeRawFormat")
      }
    }

    //remove any empty files
    val candidateFiles = HdfsUtils.listFiles(path, "*")
    HdfsUtils.removeEmptyFiles(candidateFiles, storeRawPath)

  }

  /** Write the raw data read from eventhub to hdfs */
  def writeRecords(
    appName: String,
    sqlContext:SQLContext,
    storeRawFormat: String,
    storeRawPath: String,
    storeRawMapping: ImmutableMap[String, String],
    topic: String,
    hbrows: RDD[HBRow]): Unit = {

    val path = getRecordsHdfsFilePath(topic, storeRawPath, storeRawMapping)

    val sparkSession = SparkSession.builder().getOrCreate()
    val hbrowsDF = sparkSession.createDataFrame(hbrows).toDF

    log.warn(s"StoreRawUtils: $appName: writeRawRecords: writing topic '$topic' to path '$path' format '$storeRawFormat'")

    storeRawFormat match {

      case "txt" => {
        // dont compress
        val optionsMap: Map[String, String] = Map("compression" -> "none")
        val coalesced = hbrowsDF.coalesce(1)
        coalesced.write.mode("append").options(optionsMap).text(path)
      }
      case "text" => {
        // dont compress
        val optionsMap: Map[String, String] = Map("compression" -> "none")
        val coalesced = hbrowsDF.coalesce(1)
        coalesced.write.mode("append").options(optionsMap).text(path)
      }
      case "json" => {
        // dont compress
        val optionsMap: Map[String, String] = Map("compression" -> "none")
        val coalesced = hbrowsDF.coalesce(1)
        coalesced.write.mode("append").options(optionsMap).json(path)
      }
      case "avro" =>  {
        import org.apache.spark.sql.functions.col
        // FIXME: when using coalesce and writing to azure data lake abfs:// is slow
        // hbrowsDF.printSchema
        // log.warn("HBRows schema:  " + hbrowsDF.columns.mkString(","))
        //hbrowsDF.repartition(50).write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save(path)
        // coalesced.write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save(path)
        //val coalesced = hbrowsDF.coalesce(1)
        val hbowsDFWithPartCols = hbrowsDF.withColumn("partYYYY", col("cells.partYYYY")).
          withColumn("partMM", col("cells.partMM")).
          withColumn("partDD", col("cells.partDD"))
        hbowsDFWithPartCols.write.mode(SaveMode.Overwrite).partitionBy("partYYYY", "partMM", "partDD").format("com.databricks.spark.avro").save(path)
      }
      case _ =>  {
        throw new Exception(s"Unknown save-mode $storeRawFormat")
      }

    }

    //remove any empty files
    val candidateFiles = HdfsUtils.listFiles(path, "*")
    HdfsUtils.removeEmptyFiles(candidateFiles, storeRawPath)

  }

  /** returns a path to write raw data in HDFS
    * {{
      /HDFS_ROOT/EA/supplychain/raw/ob/ORDERS/yearmonth=201907/daytimestamp=17205959/part-00000-63b71f8a-be42-4a3f-8358-0715a8addea3.txt
    * }}
    */
  def getRawHdfsFilePath(topic : String, storeRawPath:String, storeRawMapping: ImmutableMap[String, String]): String = {
    // get hdfs directory from topic-hdfs map, default to topic if map is empty or mapping is absent
    val hdfsDir = storeRawMapping.getOrElse(topic, topic)
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val pathStr  = storeRawPath + "/" + hdfsDir + "/yearmonth=" +
      simpleDateFormat.format(Instant.now().toEpochMilli).substring(0, 6) + "/daytimestamp=" +
      simpleDateFormat.format(Instant.now().toEpochMilli).substring(6, 10) + "5959"
    pathStr
  }

  def getRecordsHdfsFilePath(topic : String, storeRawPath:String, storeRawMapping: ImmutableMap[String, String]): String = {
    // get hdfs directory from topic-hdfs map, default to topic if map is empty or mapping is absent
    val hdfsDir = storeRawMapping.getOrElse(topic, topic)
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val pathStr  = storeRawPath + "/" + hdfsDir
    pathStr
  }

  /** converts rdd to a dataframe */
  def convertRDDToDF(sqlContext: SQLContext, rdd: RDD[String]): DataFrame = {
    import sqlContext.implicits._
    sqlContext.implicits.rddToDatasetHolder(rdd).toDF
  }

}
