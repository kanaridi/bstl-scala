/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * returns the result of removing duplicate records from a timestamped dataset
  */
class DeDupOp(val upstream: Table, eventTimestamp: String, ignorableCols: List[String]) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    if (upstreamDf.isEmpty)
      None
    else {

      val srcDF = upstreamDf.get

      var thisDF = if (ignorableCols.length == 0) srcDF else srcDF.drop(ignorableCols:_*)

      val thisSrcCols = thisDF.schema.fieldNames.toList

      // get source columns without timestamp columns
      val srcColsWithoutTS = thisSrcCols.filter( _ != eventTimestamp)

      // drop duplicates rows (exclude timestamp columns when determining duplicates)
      // eliminate duplicates using Window function, order by timestamp
      // only keep the oldest record
      val w = Window.partitionBy(srcColsWithoutTS.head, srcColsWithoutTS.tail:_*).orderBy(col(eventTimestamp).asc)
      thisDF = thisDF.withColumn("rn", row_number.over(w)).where(col("rn") === 1).drop("rn") // select oldest record

      thisDF = if (ignorableCols.length > 0) thisDF.join(srcDF, "left") else thisDF
      Some(thisDF)

    }
  }
}

