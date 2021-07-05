/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * returns DataFrame to caller, feeds that same dataframe to alternate Sinks
  */
case class TeeOp(upstream: Table)
    extends SingleOp
{

  var alternates: Option[List[Table]] = None

  var lastDf: Option[DataFrame] = None


  // def send(dfo: Option[DataFrame]): Unit = {
  //   // println("TeeOp: send to alternateSink")
  //   alternateSink foreach {
  //     sink => {
  //       // println(s"TeeOp: has an alternateSink of $sink")
  //       sink match {
  //         case sinkOp: Sink => {
  //           // println("TeeOp: alternateSink is a Sink")
  //           dfo foreach {
  //             // println("TeeOp: DataFrame for sending is not empty")
  //             df => sinkOp.send(df)

  //           }
  //         }
  //         case _ => Unit
  //       }
  //     }
  //   }
  // }

  override def all() = {
    lastDf = operate1(upstream.all)
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
    lastDf = operate1(upstream.newest(till))
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
    lastDf = operate1(upstream.initial(from, till))
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

/** the second sink of a Tee operator */
case class TeeOpSecondSink(upstream: Table) extends SingleOp {

  override def all() = upstream.asInstanceOf[TeeOp].lastDf
  override def newest(till: String) =     upstream.asInstanceOf[TeeOp].lastDf
  override def initial(from: String, till: String) =     upstream.asInstanceOf[TeeOp].lastDf

}
