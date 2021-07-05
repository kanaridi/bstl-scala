/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * returns DataFrame to caller, feeds that same dataframe to a second Sink
  */
case class SplitOp(val upstream: Table, val expression: String)
    extends SingleOp
{

  var alternates: Option[List[Table]] = None

  var lastDf: Option[DataFrame] = None
  var upDf:  Option[DataFrame] = None
  var errDf:  Option[DataFrame] = None

  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    if (upstreamDf.isEmpty) None else Some(upstreamDf.get.filter(expression))
  }

  override def all() = {

    lastDf.foreach(x => x.unpersist)
    upDf.foreach(x => x.unpersist)

    upDf = upstream.all
    upDf.foreach(x => x.persist)

    lastDf = operate1(upDf)
    lastDf.foreach(x => x.persist)
    errDf = Some(upDf.get.except(lastDf.get))

    alternates.get.foreach( target =>
      target match {
        case sinkOp: Sink => {
          sinkOp.all().foreach(df => sinkOp.send(df))
        }
        case _ => Unit
      }
    )
    lastDf
  }

  override def newest(till: String) = {
    lastDf.foreach(x => x.unpersist)
    upDf.foreach(x => x.unpersist)

    upDf = upstream.newest(till)
    upDf.foreach(x => x.persist)

    lastDf = operate1(upDf)
    lastDf.foreach(x => x.persist)
    errDf = Some(upDf.get.except(lastDf.get))

    lastDf = operate1(upDf)
    alternates.get.foreach( target =>
      target match {
        case sinkOp: Sink => {
          sinkOp.newest(till).foreach(df => sinkOp.send(df))
        }
        case _ => Unit
      }
    )
    lastDf
  }
  
  override def initial(from: String, till: String) = {
    lastDf.foreach(x => x.unpersist)
    upDf.foreach(x => x.unpersist)

    upDf = upstream.initial(from, till)
    upDf.foreach(x => x.persist)

    lastDf = operate1(upDf)
    lastDf.foreach(x => x.persist)
    errDf = Some(upDf.get.except(lastDf.get))

    alternates.get.foreach( target =>
      target match {
        case sinkOp: Sink => {
          sinkOp.initial(from, till).foreach(df => sinkOp.send(df))
        }
        case _ => Unit
      }
    )
    lastDf
  }
}

/** the second sink of a Split operator */
case class SplitOpSecondSink(upstream: Table) extends SingleOp {

  override def all() = upstream.asInstanceOf[SplitOp].errDf
  override def newest(till: String) =     upstream.asInstanceOf[SplitOp].errDf
  override def initial(from: String, till: String) =     upstream.asInstanceOf[SplitOp].errDf

}
