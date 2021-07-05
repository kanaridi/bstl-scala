/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * returns the freshest row for each key in a dataframe
  */
class FreshestRowsOp(val upstream: Table, keyCols: List[String], timestamp: String) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    if (upstreamDf.isEmpty)
      None
    else {
      val fromRows = upstreamDf.get
      val fSelect = fromRows.groupBy(keyCols.head, keyCols.tail:_*).
        agg(max(timestamp).cast("long").cast("string").alias(timestamp))
      val joinFields = keyCols.toSeq :+ timestamp
      Some(fSelect.join(fromRows, joinFields, "inner"))
    }
  }
}

