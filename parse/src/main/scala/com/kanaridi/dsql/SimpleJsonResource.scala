/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  *
  */
case class SimpleJsonResource(val spark: org.apache.spark.sql.SparkSession,
  val name: String,
  val path: String)
    extends Table
{

  override def all() = {
    println(s"path is $path")
    val uri = getClass.getResource(path).toString()
    println(s"uri is $uri")
    val df = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load(uri)
    Some(df)
  }

}
        
