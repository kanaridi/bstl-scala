/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.commons.logging.{Log, LogFactory}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** keeps track of dates within a data source
  *
  */
case class TimedTable(val upstream: Table,
  val destTimestamp: Option[String],
  val startFrom: Option[String])
    extends Table
{
  val log: Log = LogFactory.getLog(this.getClass.getName)

  var fullDF: Option[DataFrame] = None
  var monthlyDF: Option[DataFrame] = None
  var latestDF: Option[DataFrame] = None
  var lastNewOption: Option[Option[DataFrame]] = None

  /** return any new rows for this month, not previously processed
    *
    */
  def getNew(when: String, writer: Option[(DataFrame, String) => Long] = None) : Option[DataFrame] = {
    val yymm = when.substring(0, 6)

    val month = upstream.all
    month match {
      case Some(df) => {
        val limited1 = destTimestamp match {
          case Some(ts) => df.filter(s"$ts <= '$when'")
          case None => df
        }

        val limited = limited1.checkpoint

        val newDF1 = monthlyDF match {
          case Some(prev) => {
            limited.except(prev)
          }
          case None => limited
        }

        val newDF = newDF1.checkpoint

        latestDF.foreach(old => old.unpersist)
        latestDF = Some(newDF)

        if (newDF.count > 0) {

          monthlyDF.foreach(old => old.unpersist)
          monthlyDF = Some(limited)

          val newFull1 = destTimestamp match {
            case Some(x) => fullDF.get.union(newDF)
            case None => fullDF.get.union(newDF).dropDuplicates
          }

          val newFull = newFull1.checkpoint 

          fullDF.foreach(old => old.unpersist)
          fullDF = Some(newFull)

          Some(newDF)
        } else {
          // there were no new rows
          limited.unpersist()
          None
        }
      }
      case None => None   // failed to read any rows
    }
  }

  /** load all the rows from a month bounded block of updates, and return the most recent updates for all rows
    *
    */
  def loadInitial(startMonth: String, endMonth: String, writer: Option[(DataFrame, String) => Long] = None): Option[DataFrame] = {

    val endYY = endMonth.substring(0,4).toInt
    val endMM = endMonth.substring(4,6).toInt

    val fromMonth = startFrom.getOrElse(startMonth)
    var startYY = fromMonth.substring(0,4).toInt
    var startMM = fromMonth.substring(4,6).toInt

    log.debug(s"loadInitial $startYY $startMM thru $endYY $endMM")

    while (f"$startYY${startMM}%02d" <= f"$endYY${endMM}%02d") {

      var start = System.currentTimeMillis()

      val newDF = upstream.all
      newDF match {
        case Some(df) => {
          // df.persist
          val fDf = if (startYY == endYY && startMM == endMM && endMonth.length > 6) {
            destTimestamp match {
              case Some(ts) => df.filter(s"$ts <= '$endMonth'")
              case None => df
            }
          } else {
            df
          }

          val newFull = fullDF match {
            case Some(x) => x.union(fDf)
            case None => fDf
          }
          latestDF.foreach(old => old.unpersist)
          latestDF = Some(fDf)
          fullDF = Some(newFull)
        }
        case None => {}
      }

      var duration = System.currentTimeMillis() - start

      if (startMM == 12) {
        startMM = 1
        startYY = startYY + 1
      } else {
        startMM = startMM +1
      }
    }
    val preReduce = fullDF.get

    var start = System.currentTimeMillis()
    val postReduce = preReduce.persist

    var duration = System.currentTimeMillis() - start
    fullDF.foreach(old => old.unpersist)
    fullDF = Some(postReduce)
    fullDF
  }

  override def all() = {
    lastNewOption = None
    fullDF
  }
  override def initial(since: String, till: String) = loadInitial(since, till)
  override def newest(till: String) = {
    val nextNewDF = lastNewOption match {
      case Some(x) => x
      case None => getNew(till)
    }
    lastNewOption = Some(nextNewDF)
    nextNewDF
  }

}
