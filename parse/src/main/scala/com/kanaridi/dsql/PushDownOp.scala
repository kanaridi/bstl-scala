/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** pushes a list of columns into a single named structure, optionally aggregating across rows into an array
  * 
  */
class PushDownOp(val upstream: Table, colName: String, cols: List[String], asArray: Boolean) extends SingleOp {
  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    if (upstreamDf.isEmpty)
      None
    else {
      val upDf = upstreamDf.get
      val withStruct = upDf.withColumn(colName, struct(cols.head, cols.tail:_*))

      if (asArray) {
        val remainingCols = upDf.columns.toSeq.diff(cols)
        Some(withStruct.groupBy(remainingCols.head, remainingCols.tail:_*).agg(collect_list(colName).as(colName)))
      } else {
        val remainingCols = withStruct.columns.toSeq.diff(cols)
        Some(withStruct.select(remainingCols.head, remainingCols.tail:_*))
      }
    }
  }
}
