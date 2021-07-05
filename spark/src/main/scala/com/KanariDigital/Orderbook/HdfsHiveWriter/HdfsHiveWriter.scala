/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook.HdfsHiveWriter

import java.text.SimpleDateFormat
import java.time.Instant

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.util.{Failure, Success, Try}

case class HdfsHiveWriterException(val message: String) extends RuntimeException(message)

abstract class HdfsHiveWriter(val config : HdfsHiveWriterConfig, val sqlContext: SQLContext) extends Serializable {
  protected val log: Log = LogFactory.getLog(this.getClass.getName)
  protected val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  protected def asDF(rdd: RDD[String]): DataFrame = {
    import sqlContext.implicits._
    sqlContext.implicits.rddToDatasetHolder(rdd).toDF
  }

  private def offsetMessage(offsetRange : OffsetRange) : String = {
    "[partition: " + offsetRange.partition+" from: " +  offsetRange.fromOffset.toString() +
      " until: " + offsetRange.untilOffset.toString() + "]"
  }

  def write(topic : String, rdd: RDD[String]) : Unit = {

    rdd.persist()

    log.info(s"HdfsHiveWriter: Entering HDFS Writer with topic $topic")

    // if (rdd.isEmpty) {
    //   log.info(s"No data to write for topic $topic")
    //   return
    // } else {
    log.info(s"Writing data for topic $topic")
    writeRecords(topic, rdd)
    // }

    log.info(s"HdfsHiveWriter: Done HDFS Writer with topic $topic")
    rdd.unpersist()
  }

  protected def getHdfsFilePath(topic : String) : String = {
    val hivetable = config.hiveTopicMapping(topic)
    if (hivetable.isDefined == false)
      throw new HdfsHiveWriterException(s"Unable to determine hive table mapping for topic '$topic'")
    if (!config.hdfsRaw.isDefined)
      throw new HdfsHiveWriterException("Configuration does not specify hdfs-raw")
    val path  = "hdfs://" + config.hdfsRaw.get + "/" + hivetable.get + "/" +
      simpleDateFormat.format(Instant.now().toEpochMilli).substring(0, 6) + "/" +
      simpleDateFormat.format(Instant.now().toEpochMilli).substring(6, 10) + "5959"
    path
  }

  def writeRecordsAsCoalescedDF(coalesced: Dataset[Row], path: String) : Unit

  def writeRecords(topic: String, topicRecords: RDD[String]): Unit = {
    val path = getHdfsFilePath(topic)
    log.info(s"writing topic '$topic' to path '$path'")
    val coalesced = asDF(topicRecords).coalesce(1)
    writeRecordsAsCoalescedDF(coalesced, path)
  }

}
class HdfsHiveTextWriter(config : HdfsHiveWriterConfig, sqlContext : SQLContext) extends HdfsHiveWriter(config, sqlContext) {
  override def writeRecordsAsCoalescedDF(coalesced: Dataset[Row], path: String): Unit = {
    log.info(s"HdfsHiveTextWriter Starting to append to $path")
    coalesced.write.mode("append").text(path)
    log.info(s"HdfsHiveTextWriter Finished appending to $path")

  }
}

class HdfsHiveJsonWriter(config : HdfsHiveWriterConfig, sqlContext : SQLContext) extends HdfsHiveWriter(config, sqlContext) {
  override def writeRecordsAsCoalescedDF(coalesced: Dataset[Row], path: String): Unit = {
      coalesced.write.mode("append").json(path)
  }
}

class HdfsHiveAvroWriter(config : HdfsHiveWriterConfig, sqlContext : SQLContext) extends HdfsHiveWriter(config, sqlContext) {
  override def writeRecordsAsCoalescedDF(coalesced: Dataset[Row], path: String): Unit = {
    coalesced.write.mode(SaveMode.Append).format("com.databricks.spark.avro").save(path)
  }
}

object HdfsHiveWriterFactory extends Serializable {
  protected val log: Log = LogFactory.getLog("HdfsHiveWriterFactory")

  def construct(config : HdfsHiveWriterConfig, sqlContext : SQLContext) : Try[HdfsHiveWriter] = {
    var saveModeDefault = "txt"
    if (config.saveMode.isDefined == false) {
      log.warn(s"HDFS Hive Writer save-mode not specified, using ddefault: $saveModeDefault")
    }
    val saveMode = config.saveMode.getOrElse(saveModeDefault).toLowerCase
    saveMode match {
      case "txt" => Success(new HdfsHiveTextWriter(config,sqlContext))
      case "json" => Success(new HdfsHiveJsonWriter(config,sqlContext))
      case "avro" =>Success(new HdfsHiveAvroWriter(config,sqlContext))
      case _ => Failure(new HdfsHiveWriterException(s"Unknown save-mode $saveMode"))
    }
  }
}








