/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * returns DataFrame from a jdbc source table or query
  */
case class JdbcTable(val spark: org.apache.spark.sql.SparkSession,
  val sqlUrl: String,
  val sqlUser: String,
  val sqlPass: String,
  val driver: String,
  val sqlTable: Option[String],
  val sqlQuery: Option[String]
)
    extends Table
{

  override def all() = {

    val df1 = spark.read
      .format("jdbc")
      .option("url", sqlUrl)
      .option("user", sqlUser)
      .option("password", sqlPass)
      .option("driver", driver)

    val df2 = sqlTable match {
      case Some(tableName) => df1.option("dbtable", tableName)
      case None => df1
    }


    val df3 = sqlQuery match {
      case Some(query) => df2.option("query", query)
      case None => df2
    }

    Some(df3.load())
  }

}

