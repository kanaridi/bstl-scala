/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.util.Calendar
import java.text.SimpleDateFormat

/**
  *
  */
class Sink(val spark: org.apache.spark.sql.SparkSession ,
  val name: String,
  val upstream: Table,
  val prefetchFrom: String,
  val prefetchTo: String,
  val save: Boolean,
  val catchUpIncrement: String,
  val recheckPause: Long)
    extends SingleOp
{

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** the function we'll use for writing output */
  var send: (DataFrame) => Long = defaultWriter

  /** the upstream DataFrame, unaltered
    */
  override def operate1(upstreamDf: Option[DataFrame]) = upstreamDf

  override def run() = {

    var message = s"Sink run from: $prefetchFrom, to: $prefetchTo"
    log.info(message)

    val rdfOpt = initial(prefetchFrom, prefetchTo)

    rdfOpt match {
      case Some(rdf) => {
        if (save) {
          send(rdf)
        } else {
          0
        }
      }
      case None => 0
    }
    catchUp(prefetchTo, (recheckPause > 0))
  }

  def defaultWriter(df: DataFrame): Long = { 0 }

  /** update sink target from the last starting date.
    *  Process one day at a time till caught up, then, if @continue,
    *   keep checking for new updates.
    */
  def catchUp(lastDay: String, continue: Boolean = true) : Long = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val fdf = new SimpleDateFormat("yyyyMMddHHmmss")

    val reDays = "([0-9]+) ([dD][aA][yY][sS]?)".r
    val reWeeks = "([0-9]+) ([wW][eE][eE][kK][sS]?)".r

    var message = s"Sink catchUp to: $lastDay"
    log.info(message)

    val cal = Calendar.getInstance
    cal.setTime(sdf.parse(lastDay))

    var now = Calendar.getInstance
    var idocs : Long = 0

    while (cal.getTime.compareTo(now.getTime) < 0 ) {

      cal.add(Calendar.DATE, 1)
      val thisTime = fdf.format(cal.getTime)

      log.warn(s"catching up to $thisTime\n")

      val start = System.currentTimeMillis()

      val rdfOpt = newest(thisTime)
      idocs += {
        rdfOpt match {
          case Some(rdf) => {
            log.warn(s"sink: run(): going to save : newest() returned ${rdfOpt.get.count} rows")
            send(rdf)
          }
          case None => {
            log.warn(s"sink: run(): newest() returned nothing")
            0
          }
        }
      }
      val duration = System.currentTimeMillis() - start
    }

    while (continue) {

      log.warn(s"napping for $recheckPause ms\n")
      Thread.sleep(recheckPause)

      now = Calendar.getInstance
      val thisTime = fdf.format(now.getTime)

      log.warn(s"catching up to $thisTime\n")
      val start = System.currentTimeMillis()

      val rdfOpt = newest(thisTime)
      idocs += {
        rdfOpt match {
          case Some(rdf) => send(rdf)
          case None => 0
        }
      }
    }
    idocs
  }

}
