/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
// import org.apache.spark.sql.functions._

/**
  *
  */
class PopStruct(val upstream: Table, col: String) extends SingleOp {
  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    val colexpr = s"${col}.*"
    if (upstreamDf.isEmpty) None else Some(upstreamDf.get.select(colexpr))
  }
}

        
