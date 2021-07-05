/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  *
  */
case class SimpleJsonFile(val spark: org.apache.spark.sql.SparkSession,
  val name: String,
  val path: String)
    extends Table
{

  override def all() = {
    println(s"path is $path")
    val df = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load(path)
    Some(df)
  }

}
