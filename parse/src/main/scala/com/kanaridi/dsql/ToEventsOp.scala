/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * returns the result of a two table join as a set of timestamped events
  */
class ToEventsOp(val upstream: Table, keyCols: List[String], leftTimestamp: String, rightTimestamp: String, eventTimestamp: String) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    if (upstreamDf.isEmpty)
      None
    else {
      /*
       upstream is the result of joining two event sets. 

       We start with rows that contains key_1, key_2 .. key_n, leftTimestamp, rightTimestamp, 
       and any number of other columns. leftTimestamp and rightTimestamp are the timestamp
       columns from the left and right sides of the upstream join 

       eventTimestamp is calculated as the greatest of leftTimestamp and rightTimestamp.

       For any given <key_[1 ... n], eventTimestamp> (the stuff that uniquely identifies an event) 
       we may have multiple rows with different data and different leftTimestamp and/or rightTimestamp 
       values.

       We want to return only one row for each given <key:_[1 ... n], eventTimestamp>

       That row contains the two highest leftTimestamp and rightTimestamp available for that key
       */

      val fromRows = upstreamDf.get.
        withColumn(eventTimestamp, expr(s"greatest($leftTimestamp, $rightTimestamp)"))


      // collapse the rightTimestamp to the biggest for our keys 
      val kcolsl = keyCols :+ eventTimestamp :+ leftTimestamp
      val maxRights = fromRows.groupBy(kcolsl.head, kcolsl.tail:_*).
        agg(max(rightTimestamp).alias(rightTimestamp))


      // next, collapse the leftTimestamp to the biggest for our keys
      val kcols = keyCols :+ eventTimestamp :+ rightTimestamp
      val maxBoth = maxRights.groupBy(kcols.head, kcols.tail:_*).
        agg(max(leftTimestamp).alias(leftTimestamp))

      // maxBoth now contains only one row for each key_[1, ... n], eventTimestamp
      // we need to join it with the original set to pick up all the data columns

      val joinFields = keyCols.toSeq :+ eventTimestamp :+ leftTimestamp :+ rightTimestamp

      Some(maxBoth.join(fromRows, joinFields, "inner"))
    }
  }
}

