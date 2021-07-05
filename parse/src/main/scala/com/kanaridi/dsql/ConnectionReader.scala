/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * returns DataFrame of CVS, JSON, AVRO, ORC, PARQUET or TEXT from HDFS or ADLS blob store 
  */
case class ConnectionReader(val spark: org.apache.spark.sql.SparkSession,
  val name: String,
  val path: String,
  val format: String,
  val options: Map[String, String]
)
    extends Table
{

  override def all() = {
    // println(s"SimpleFile: path is $path")
    // println(s"SimpleFile: format is $format")
    // println(s"SimpleFile: options are is $options")

    val df = spark.read
      .format(format)
      .options(options)
      .load(path)

    Some(df)
  }

}
