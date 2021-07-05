/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.commons.logging.{Log, LogFactory}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** converts an SAP Hana table's Avro export to a DataFrame with HPE semantic names
  *
  */
case class AvroTable(val spark: org.apache.spark.sql.SparkSession,
  val name: String,
  val path: String,
  val srcCols: List[String],
  val destCols: List[String],
  val srcTimestamp: Option[String],
  val destTimestamp: Option[String],
  val keyFields: Option[List[String]],
  val filterExpr: Option[String])
{
  val log: Log = LogFactory.getLog(this.getClass.getName)
  val dfReader = spark.read.format("com.databricks.spark.avro")

  var fullDF: Option[DataFrame] = None
  var monthlyDF: Option[DataFrame] = None
  var latestDF: Option[DataFrame] = None

  val projectFrom = {
    srcTimestamp match {
      case Some(x) => srcCols :+ x
      case None => srcCols
    }
  }

  val projectTo : List[String] = {
    destTimestamp match {
      case Some(x) => destCols :+ x
      case None => destCols
    }
  }

  /** load a dataframe from AVRO, selecting the input columns and mapping to output
    *
    */
  def loadDF(path: String) : Option[DataFrame] = {
    try {
      var thisDF = dfReader.load(path)
        .select(projectFrom.head, projectFrom.tail:_*)
        .toDF(projectTo:_*)

      thisDF = filterExpr match {
        case Some(x) => thisDF.filter(filterExpr.get)
        case None => thisDF
      }

      destTimestamp match {
        case Some(x) => Some(thisDF.groupBy(destCols.head, destCols.tail:_*)
            .agg(min(x).cast("long").cast("string").alias(x)))
        case None => Some(thisDF)
      }

    } catch {
      case e: org.apache.spark.sql.AnalysisException => {
        log.warn(e)
        None
      }
      case e: java.io.FileNotFoundException => {
        log.warn(e)
        None
      }
        None
    }
  }

  /** returns only the latest version of each row by rowkey
    *
    */
  def freshRows(fromRows: DataFrame) : DataFrame = {

    fromRows

    // keyFields match {
    //   case Some(kf) =>  {
    //     destTimestamp match {
    //       case Some(ts) => {
    //         val fSelect = fromRows.groupBy(kf.head, kf.tail:_*).agg(max(ts).cast("long").cast("string").alias(ts))
    //         val joinFields = kf.toSeq :+ ts
    //         fSelect.join(fromRows, joinFields, "inner")
    //       }
    //       case None => fromRows
    //     }
    //   }
    //   case None => fromRows
    // }
  }

  // def setDFName(df: DataFrame, name: String): DataFrame = {
  //   df.createOrReplaceTempView(name)
  //   df.sparkSession.catalog.cacheTable(name)
  //   df
  // }

  /** return any new rows for this month, not previously processed
    *
    */
  def getNew(when: String, writer: Option[(DataFrame, String) => Long] = None) : Option[DataFrame] = {
    val yymm = when.substring(0, 6)
    val fullPath = s"${path}/yearmonth=${yymm}"

    val month = loadDF(fullPath)
    month match {
      case Some(df) => {
        val limited1 = destTimestamp match {
          case Some(ts) => freshRows(df.filter(s"$ts <= '$when'")) //.sort(keyFields.get.head, keyFields.get.tail:_*)
          case None => df
        }
        // val limited = setDFName(limited1, s"limited_$yymm").persist
        val limited = limited1.checkpoint//persist
                                         // limited1.persist
        log.warn(s"monthly count up to $when is ${limited.count} rows of $name")

        val newDF1 = monthlyDF match {
          case Some(prev) => {
            limited.except(prev)
          }
          case None => limited
        }

        //val newDF = setDFName(newDF1, s"newDF_$yymm").persist //newDF1.persist
        val newDF = newDF1.checkpoint //persist
                                      // newDF.persist

        latestDF.foreach(old => old.unpersist)
        latestDF = Some(newDF)

        log.warn(s"found ${newDF.count} rows of $name")
        newDF.show(20)

        if (newDF.count > 0) {
          val written = writer match {
            case Some(wfunc) => {
              wfunc(newDF, this.name)
            }
            case None => 0
          }

          monthlyDF.foreach(old => old.unpersist)
          monthlyDF = Some(limited)


          val newFull1 = destTimestamp match {
            case Some(x) => freshRows(fullDF.get.union(newDF)) // .sort(keyFields.get.head, keyFields.get.tail:_*)
            case None => fullDF.get.union(newDF).dropDuplicates
          }

          //val newFull = setDFName(newFull1, s"newFull_$yymm").persist//newFull1.persist
          val newFull = newFull1.checkpoint //persist
                                            // newFull.persist
          log.warn(s"now holding ${newFull.count} rows of $name")

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
  def loadInitial(startMonth: String, endMonth: String, writer: Option[(DataFrame, String) => Long] = None): DataFrame = {

    val endYY = endMonth.substring(0,4).toInt
    val endMM = endMonth.substring(4,6).toInt
    var startYY = startMonth.substring(0,4).toInt
    var startMM = startMonth.substring(4,6).toInt

    log.warn(s"loadInitial $startYY $startMM thru $endYY $endMM")

    while (startYY < endYY || startMM <= endMM) {

      val fullPath = f"${path}/yearmonth=${startYY}${startMM}%02d"

      var start = System.currentTimeMillis()

      log.warn(s"loading $fullPath")

      val newDF = loadDF(fullPath)
      newDF match {
        case Some(df) => {
          // df.persist
          val fDf = if (startYY == endYY && startMM == endMM && endMonth.length > 6) {
            destTimestamp match {
              case Some(ts) => freshRows(df.filter(s"$ts <= '$endMonth'"))
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
      log.warn(s"done loading $fullPath")

      if (startMM == 12) {
        startMM = 1
        startYY = startYY + 1
      } else {
        startMM = startMM +1
      }
    }
    val preReduce = fullDF.get
    log.warn(s"loading freshRows for ${this.name}")

    var start = System.currentTimeMillis()
    val postReduce = freshRows(preReduce).persist

    log.warn(s"done loading freshRows ${postReduce.count} rows for ${this.name}")

    var duration = System.currentTimeMillis() - start
    fullDF.foreach(old => old.unpersist)
    fullDF = Some(postReduce)
    postReduce
  }
}
