/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * returns the result of a pipeline specified in another file 
  */
class SubFlowOp(val upstream: Table, sourceSpec: String) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    if (upstreamDf.isEmpty)
      None
    else {
      None
    }
  }
}

