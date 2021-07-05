/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._

import java.text.SimpleDateFormat
import org.apache.spark.sql.expressions.Window

import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters

import java.time.ZonedDateTime
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId

import java.util.Date
import java.util.TimeZone

import com.kanaridi.common.util.HdfsUtils
import com.kanaridi.common.util.LogUtils
import com.kanaridi.common.util.LogUtils.BugType
import java.time.format.DateTimeParseException


/** converts a csv export to a DataFrame
  *
  */
case class TimePartitionedCsvTable(val spark: org.apache.spark.sql.SparkSession,
  val name: String,
  val path: String,
  var srcCols: List[String],
  var destCols: List[String],
  val srcTimestamp: Option[List[String]],
  val destTimestamp: Option[String],
  val keyFields: Option[List[String]],
  val startFrom: Option[String],
  val srcTimestampFormat: Option[String],
  val srcTimezone: Option[String],
  val readInitPath: Option[String],
  val header: Option[String],

  val separator: Option[String],
  val inferSchema: Option[String],
  val apiTest: Boolean,
  val previewMode: Boolean,
  val embargo: Boolean)
    extends Table
{
  val log: Log = LogFactory.getLog(this.getClass.getName)

  val destTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
  // val destTimestampFormat = "yyyyMMddHHmmss"
  val destTimezone = "UTC"

  val readOptions = Map(
    "header" -> header.getOrElse("true"),
    "sep" -> separator.getOrElse(","),
    "inferSchema" -> inferSchema.getOrElse("false"),
    "mode" -> "FAILFAST"
  )

  var fullDF: Option[DataFrame] = None
  var monthlyDF: Option[DataFrame] = None
  var latestDF: Option[DataFrame] = None
  var lastNewOption: Option[Option[DataFrame]] = None

  /** given dataframe find latest state of all the rows by primary key fields */
  def currentState(fromRows: DataFrame, keyfields: Option[List[String]], destTimestamp: Option[String]) : DataFrame = {
    keyFields match {
      case Some(x) =>  {
        destTimestamp match {
          case Some(ts) => {

            // convert keyFields into avro compliant names, before quering dataframe
            val kf = toAvroNames(x)

            // log.warn(s"""currentState: keyFields: ${kf.mkString(",")}""")
            // log.warn(s"currentState: destTimestamp: $ts")

            // cast timestamp with alias kmax_ts - prevent two timestamp columns with same name
            // (happpens when key field is also a timestamp field)
            val fSelect = fromRows.groupBy(kf.head, kf.tail:_*).agg(max(ts).cast("string").alias("kmax_ts"))
            val joinFields = kf.toSeq :+ "kmax_ts"

            // decorate original data with kmax_ts
            val fromRowsDec = fromRows.withColumn("kmax_ts", col(ts)).dropDuplicates
            val currentState = fSelect.join(fromRowsDec, joinFields, "inner").drop("kmax_ts")

            currentState
          }
          case None => fromRows
        }
      }
      case None => fromRows
    }
  }

  /** read landed schema if available */
  def readLandedSchema(readInitPath: Option[String]): Option[String] = {
    Try {
      val fc = FileContext.getFileContext()
      // check if schema exists
      val baseDir = new Path(readInitPath.get)
      val schemaPath = HdfsUtils.resolvePaths(baseDir, baseDir.getName() + ".avsc")
      if (fc.util.exists(schemaPath)) {
        log.warn(s"reading landed schema from  $schemaPath ...")
        HdfsUtils.readFileContents(schemaPath)
      } else {
        val message = s"reading landed schema $schemaPath failed, schema does not exist..."
        log.warn(message)
        throw new Exception(message)
      }
    } match {
      case Success(x) => Some(x.mkString)
      case Failure(ex) => {
        log.warn(s"cannot read landed schema got an exception ${ex.getMessage}" )
        None
      }
    }
  }

  def readInitFullDF(readInitPath: Option[String]): Option[DataFrame] = {
    // assuming that landed table data is in avro format
    Try {
      readLandedSchema(readInitPath) match {
        case Some(avroSchema) => {
          log.warn(s"found and read avro schema: $avroSchema")
          // Spark 2.4 - set ignoreExtension to false, to only read files with .avro extension
          val readOptions = Map("avroSchema" -> avroSchema, "ignoreExtension" -> "false")
          val readPath = readInitPath.get
          val readDF = spark.read.format("com.databricks.spark.avro").options(readOptions).load(readPath)
          readDF.show(20)
          currentState(readDF, keyFields, destTimestamp)
        }
        case None => {
          log.warn(s"avro schema not found")
          val readPath = readInitPath.get
          // Spark 2.4 - set ignoreExtension to false, to only read files with .avro extension
          val readOptions = Map("ignoreExtension" -> "false")
          val readDF = spark.read.format("com.databricks.spark.avro").options(readOptions).load(readPath)
          readDF.show(20)
          currentState(readDF, keyFields, destTimestamp)
        }
      }
    } match {
      case Success(readDFCurrentState) => {
        log.warn(s"readInitFullDF: get currentState found ${readDFCurrentState.count} rows")
        readDFCurrentState.show(20)
        fullDF = Some(readDFCurrentState)
        monthlyDF = Some(readDFCurrentState)
        fullDF
      }
      case Failure(ex) => {
        log.warn(s"readInitFullDF: cannot read init data from $readInitPath error ${ex.getMessage}")
        None
      }
    }
  }

  def readCSVFile(filePath: String): Try[DataFrame] = Try {
    val dfReader = if (srcCols.isEmpty) {
      spark.read.format("csv").options(readOptions)
    } else {
      val customSchema = StructType(srcCols.map(fieldName => StructField(fieldName.trim, StringType, true)))
      spark.read.format("csv").options(readOptions).schema(customSchema)
    }
    dfReader.load(filePath)
  }

  /** get start of formatted epoch */
  def getErrorTS(): String = {
    val sdf = new SimpleDateFormat(destTimestampFormat)
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    val errorTS = sdf.format(new Date(0))
    errorTS
  }

  /** iterate over all files starting at a directory and decorate
    * dataframe with FILENAME, LAST_MODIFIED
    */
  def iterateFiles(incomingDir: String): Try[DataFrame] = {
    // get an iterator and start processing files in depth first order
    Try {
      val fc = FileContext.getFileContext()
      fc.util.listFiles(new Path(incomingDir), true)
    } match {
      case Success(x) => {
        var myDF: Option[DataFrame] = None
        val filesIter: RemoteIterator[LocatedFileStatus] = x

        val dqChecker = new DQCheck(spark, readOptions, this)

        var filesReadCount = 0

        val MAX_PREVIEW_FILE_COUNT = 1

        // extract last modified, filename timestamp by reading file at a time
        while ((filesIter.hasNext && previewMode && filesReadCount < MAX_PREVIEW_FILE_COUNT)
          || (filesIter.hasNext && !previewMode)) {

          val locatedFileStatus = filesIter.next()
          val lastModified = locatedFileStatus.getModificationTime
          val locatedFileDir = locatedFileStatus.getPath.getParent.toString
          val locatedFilename = locatedFileStatus.getPath.getName.toString
          val locatedFilePath = locatedFileStatus.getPath.toString

          // last modified timestamp format
          val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
          val lastModifiedFormatted = simpleDateFormat.format(lastModified)

          // get files that are not zero bytes
          if (locatedFileStatus.isFile && locatedFileStatus.getLen > 0) {

            // check data quality
            dqChecker.checkFileDQ(locatedFilePath, "CSV")
            // select rows with row keys
              .flatMap(df => selectRowsWithKeys(df))
            // remove empty columns
              .flatMap(df1 => removeEmptyColumns(df1))
            // rename columsn to avro friendly
              .flatMap(df2 => renameColumnsToAvroFriendly(df2))
            // add _TS columns
              .flatMap(df3 => Try {
                df3.withColumn("FILENAME_TS", lit(locatedFilename))
                  .withColumn("LAST_MODIFIED_TS", lit(lastModifiedFormatted))
              }) match {
                case Success(oneDF) => {
                  myDF match {
                    case Some(x) => {
                      myDF = if (x.count > 0) Some(doUnion(x, oneDF)) else Some(oneDF)
                    }
                    case None => {
                      myDF = Some(oneDF)
                    }
                  }
                  log.warn(s"read file: ${locatedFilePath}, cummulative records read: ${myDF.get.count}")
                }
                case Failure(valEx) => {
                  // do nothing
                  log.warn(s"${locatedFileStatus.getPath.toString} failed **data** quality check: ${valEx.getMessage} +++")

                  val (errorType, errorMessage, errorStack) = LogUtils.getExceptionDetailsWithType(valEx)
                  if (apiTest) {
                    //
                    val errorDF = readCSVFile(locatedFileStatus.getPath.toString) match {
                      case Success(x) => {
                        val errorTS = getErrorTS()

                        var eDF = x.withColumn("FILENAME_TS", lit(locatedFilename))
                          .withColumn("LAST_MODIFIED_TS", lit(lastModifiedFormatted))

                        // if possible add DERIVED_TS
                        eDF = formatDerivedTS(eDF);
                        eDF = eDF.withColumn("DERIVED_TS", when(col("DERIVED_TS").isNull, errorTS).otherwise(col("DERIVED_TS")))

                        eDF = eDF.withColumn("__errortype__", lit(errorType))
                        eDF = eDF.withColumn("__error__", lit(errorMessage))
                        eDF = eDF.withColumn("__errorstack__", lit(errorStack))

                        eDF
                      }
                      case Failure(e) => {
                        val (errorType, errorMessage, errorStack) = LogUtils.getExceptionDetailsWithType(e)
                        import spark.implicits._
                        val errorTS = getErrorTS()
                        val eDF = Seq(
                          (locatedFilename, lastModifiedFormatted, errorTS, errorType, errorMessage, errorStack)
                        ).toDF("FILENAME_TS", "LAST_MODIFIED_TS", "DERIVED_TS", "__errortype__", "__error__", "__errorstack__")
                        eDF
                      }
                    }

                    myDF match {
                      case Some(x) => {
                        myDF = if (x.count > 0) Some(doUnion(x, errorDF)) else Some(errorDF)
                      }
                      case None => {
                        myDF = Some(errorDF)
                      }

                    }

                  } else {
                    val runtimeApplicationId = spark.sparkContext.applicationId
                    val exceptionDetail = s"""$runtimeApplicationId - READ_RAW_CSV_ERROR -
                                             |applicationId: 1, appName: flow, appType: flow,
                                             |exceptionMessage: $errorMessage, exceptionTrace: $errorStack,
                                             |message: error reading raw file $locatedFilePath with exception $errorMessage"""
                      .stripMargin.replaceAll("\n", " ")
                    log.error(exceptionDetail)

                    if (embargo) {
                      // move file to embargo if necessary
                      valEx match {
                        case ex @ (_: DQDataException | _: DQConfigException | _: DQException | _:DQDerivedTSFormatException
                                   | _: Throwable) => {
                          embargoFile(locatedFilePath) match {
                            case Success(embargoFilePath) => {
                              LogUtils.logBug(BugType.DataBug, errorMessage, exceptionDetail, embargoFilePath)
                            }
                            case Failure(exception) => {
                              val (errorType, errorMessage, errorStack) = LogUtils.getExceptionDetailsWithType(exception)
                              val exceptionDetails = s"""type: $errorType | message: $errorMessage | trace: $errorStack"""
                              LogUtils.logBug(BugType.Bug, errorMessage, exceptionDetails)
                            }
                          }
                        }
                      }
                    }
                  }
                }
              } // readAndCheckDQ

            // increment files read count
            filesReadCount = filesReadCount + 1

          } // file exists


        } // end of while

        // return union of files read
        // myDF
        myDF match {
          case Some(x) => Success(x)
          case None => {
            val message = s"""reading data
              | from $incomingDir failed"""
              .stripMargin.replaceAll("\n", " ")
            Failure(new Exception(message))
          }
        }
      }
      case Failure(ex) => {
        log.warn(s"reading data from $incomingDir failed with message ${ex.getMessage}")
        Failure(ex)
      }
    }
  }

  /** move file to embargo directory */
  def embargoFile(locatedFilePath: String): Try[String] = Try {
    log.warn(s"embargoFile: moving $locatedFilePath to embargo...")
    val dataPath = new Path(locatedFilePath, "../../../..")
    val embargoDirName = dataPath.getName + "_embargo"
    val embargoDataPath = new Path(new Path(dataPath, ".."), embargoDirName)
    val dd = new Path(locatedFilePath, "..")
    val mm = new Path(dd, "..")
    val yyyy = new Path(mm, "..")
    val embargoFilePath = embargoDataPath + s"/${yyyy.getName}/${mm.getName}/${dd.getName}/"
    log.warn(s"embargoFile: moving file to embargo $locatedFilePath -> $embargoFilePath ...")
    val sourceFiles = new Array[String](1)
    sourceFiles(0) = locatedFilePath
    HdfsUtils.moveFiles(sourceFiles, embargoFilePath)
    log.warn(s"embargoFile: moving $locatedFilePath to embargo done.")
    embargoFilePath
  }

  /** format and add  DERIVED_TS timestamp column */
  def formatDerivedTS(thisDF: DataFrame): DataFrame = {

    var derivedTS: DataFrame = thisDF

    srcTimestamp match {
      case Some(x) => {
        // extract timestamp fields from the dataframe
        val dcols = srcTimestamp.get

        // get original columns of the data frame
        val originalCols = derivedTS.schema.fieldNames.toList

        derivedTS = derivedTS.withColumn("DERIVED_TS", lit(""))
        dcols.map(x => {
          derivedTS = derivedTS.withColumn("DERIVED_TS", concat(
            when(col("DERIVED_TS").isNotNull, col("DERIVED_TS")).otherwise(lit("")),
            lit(" "),
            when(col(x).isNotNull, col(x)).otherwise(lit(""))))
          // derivedTS.show
        })

        val formattedDFOut = derivedTS.withColumn("toTSOut",
          CustomUDF.toDerivedTS(trim(col("DERIVED_TS")), lit(srcTimestampFormat.get), lit(srcTimezone.get)))

        val selectCols = originalCols ++ List("toTSOut.DERIVED_TS", "toTSOut.PARSE_ERROR")

        // select original columnns and columns added by udf
        formattedDFOut.select(selectCols.head, selectCols.tail:_*)
      }
      case None => {
        derivedTS
          .withColumn("DERIVED_TS", lit(null).cast("string"))
          .withColumn("PARSE_ERROR", lit("DATE FORMAT ERROR source timestamp has not been specified"))
      }
    }
  }

  /** extract srcTimestamp fields and add a  dest timestamp (DERIVED_TS)
    * which is a normalized timestamp for a record
    */
  def addDerivedTS(thisDF: DataFrame): Try[DataFrame] = {

    if (thisDF.columns.toList.contains("DERIVED_TS")) {
      return Success(thisDF)
    } else {
      val formattedDF = formatDerivedTS(thisDF);
      // select rows where DERIVED_TS exists
      val formattedDFNotNull = formattedDF.filter(col("DERIVED_TS").isNotNull)
      Success(formattedDFNotNull)
    }
  }

  /** get avro names */
  def toAvroNames(names: List[String]): List[String] = {
    names.map(x => x.trim.replaceAll("[^A-Za-z0-9_]", "_")).toList
  }

  /** make avro friendly column name data frame */
  def renameColumnsToAvroFriendly(thisDF: DataFrame): Try[DataFrame] = Try {
    var renameDF = thisDF
    val fromCols = renameDF.schema.fieldNames.map(x => if (x.contains(".")) s"`$x`" else x)
    val toCols = toAvroNames(renameDF.schema.fieldNames.toList)
    renameDF = renameDF.select(fromCols.head, fromCols.tail:_*).toDF(toCols:_*)
    renameDF
  }

  /** select rows remove rows which are required fields missing */
  def selectRowsWithKeys(thisDF: DataFrame): Try[DataFrame] = {
    var selectDF = thisDF
    // check if keyFields exist
    keyFields match {
      case Some(kf) => {
        // expand the data frame add key fields
        for (x <- kf) {
          if (!selectDF.schema.fieldNames.contains(x)) {
            selectDF = selectDF.withColumn(x, lit(null).cast(StringType))
          }
        }
        // println("keyExistsCondition: " +  kf.mkString(","))
        val keyExistsCond = kf.map(x => col(x).isNotNull).reduce(_ and _)
        selectDF = selectDF.filter(keyExistsCond)
        // println("selectDF: count="+ selectDF.count)
        Success(selectDF)
      }
      case None => Failure(new Exception("keys are not defined"))
    }
  }

  def removeEmptyColumns(thisDF: DataFrame): Try[DataFrame] = Try {
    var oneDF = thisDF
    var fields = oneDF.schema.fieldNames.filter(x => !x.startsWith("_c"))
    fields = fields.map(x => if (x.contains(".")) s"`$x`" else x)
    oneDF = oneDF.select(fields.head, fields.tail:_*)
    oneDF
  }

  def removeDuplicates(df: DataFrame): Try[DataFrame] = Try {

    var thisDF = df
    log.warn("loadDF: before eliminating dups ========: count=" + thisDF.count)
    // thisDF.show(100)

    val thisSrcCols = if (srcCols.isEmpty) thisDF.schema.fieldNames.toList else srcCols

    // get source columns without timestamp columns
    val srcColsWithoutTS = getColsExcludeTS(thisSrcCols)

    // drop duplicates rows (exclude timestamp columns when determining duplicates)
    // eliminate duplicates using Window function, order by timestamp
    // only keep the oldest record
    val w = Window.partitionBy(srcColsWithoutTS.head, srcColsWithoutTS.tail:_*).orderBy(col("DERIVED_TS").asc)
    thisDF = thisDF.withColumn("rn", row_number.over(w)).where(col("rn") === 1).drop("rn") // select oldest record
    log.warn("loadDF: after eliminating dups ========: count=" + thisDF.count)
    thisDF
  }

  /** load a dataframe from csv, selecting the input columns and mapping to output
    *
    */
  def loadDF(path: String) : Option[DataFrame] = {
    iterateFiles(path)
      .flatMap(df => Try {
        val thisDF = df.checkpoint

        // set srcCols and destCols if not set
        val thisSrcCols = if (srcCols.isEmpty) thisDF.schema.fieldNames.toList else srcCols
        val thisDestCols = if (destCols.isEmpty) thisDF.schema.fieldNames.toList else destCols
        // project dataframe: srcCols to destCols
        val rdf = thisDF.select(thisSrcCols.head, thisSrcCols.tail:_*)
          .toDF(thisDestCols:_*)

        rdf
      })
      .flatMap(df2 => addDerivedTS(df2)) // data frame with normalized timestamp
      .flatMap(df3 => removeDuplicates(df3)) match {
        case Success(x) => Some(x)
        case Failure(ex) => {
          ex match {
            case e: java.io.FileNotFoundException => {
              log.warn(e)
              // e.printStackTrace
              // ignore error
              None
            }
            case e @ (_ :org.apache.spark.sql.AnalysisException | _ :Throwable) => {
              // log error
              log.warn(e)
              if (apiTest) {
                // e.printStackTrace
                import spark.implicits._
                val errorTS = getErrorTS()
                val (errorType, errorMessage, errorStack) = LogUtils.getExceptionDetailsWithType(e)
                val eDF = Seq(
                  (errorTS, errorTS, errorTS, errorType, errorMessage, errorStack)
                ).toDF("FILENAME_TS", "LAST_MODIFIED_TS", "DERIVED_TS", "__errortype__", "__error__", "__errorstack__")
                Some(eDF)
              } else {
                //ignore error
                None
              }
            }
          }
        }
      }
  }

  /** get cols excluding src and dest timestamp columns */
  def getColsExcludeTS(allCols: Seq[String]): Seq[String] = {

    // get columns without source timestamp fields
    var srcColsWithoutTS: Seq[String] = srcTimestamp match {
      case Some(srcTSCols) => allCols diff srcTSCols
      case None => allCols
    }

    // remove timestamp field from list of columns
    srcColsWithoutTS = destTimestamp match {
      case Some(destTS) => srcColsWithoutTS diff Seq(destTS)
      case None => srcColsWithoutTS
    }

    // remove *_TS columns
    srcColsWithoutTS diff Seq("LAST_MODIFIED_TS", "FILENAME_TS", "yearmonth", "daytimestamp")
  }

  /** returns only the latest version of each row by rowkey
    *
    */
  def freshRows(fromRows: DataFrame) : DataFrame = {
    fromRows
  }

  /** do union of potentially two dataframes with different columns */
  def doUnion(df1: DataFrame, df2: DataFrame) = {

    val cols1 = df1.columns.toSet
    val cols2 = df2.columns.toSet
    val allCols = cols1 ++ cols2

    val sortedAllCols = allCols.toList.sorted

    val selectExpr1 = sortedAllCols.map(x => x match {
      case x if cols1.contains(x) => col(x)
      case _ => lit(null).as(x)
    })

    val selectExpr2 = sortedAllCols.map(x => x match {
      case x if cols2.contains(x) => col(x)
      case _ => lit(null).as(x)
    })

    df1.select(selectExpr1:_*).union(df2.select(selectExpr2:_*))
  }


  /** find rows in df1 which are not in df2
    * ignore anytimestamp columns when comparing
    */
  def doExcept(df1: DataFrame, df2: DataFrame): DataFrame = {

    val cols1 = df1.columns.toSet
    val cols2 = df2.columns.toSet
    val allCols = cols1 ++ cols2

    val sortedAllCols = allCols.toList.sorted

    val selectExpr1 = sortedAllCols.map(x => x match {
      case x if cols1.contains(x) => col(x)
      case _ => lit(null).as(x)
    })

    val selectExpr2 = sortedAllCols.map(x => x match {
      case x if cols2.contains(x) => col(x)
      case _ => lit(null).as(x)
    })

    val df1Filled = df1.select(selectExpr1:_*)
    val df2Filled = df2.select(selectExpr2:_*)

    // find columns which are not timestamps
    val srcColsWithoutTS = getColsExcludeTS(df1Filled.schema.fieldNames.toList)

    // use leftanti join to get all new rows that have changed excluding timestamp columns
    // use a null safe join operator
    val nullSafeJoinConditions = srcColsWithoutTS.map(colName => {
      df1Filled(colName) <=> df2Filled(colName)
    }).reduce( _ and _ )

    df1Filled.join(df2Filled, nullSafeJoinConditions, "leftanti")

  }

  /** return any new rows for this month, not previously processed
    *
    */
  def getNew(when: String, writer: Option[(DataFrame, String) => Long] = None) : Option[DataFrame] = {

    val yy = when.substring(0, 4)
    val mm = when.substring(4, 6)

    val fullPath = s"${path}/${yy}/${mm}"

    log.warn(s"getNew: when = $when")

    val month = loadDF(fullPath)

    month match {
      case Some(df) => {
        val limited1 = destTimestamp match {
          // look for new rows by 'when'
          case Some(ts) => {
            // convert when to yyyy-mm-dd format
            val wdt = new SimpleDateFormat("yyyyMMdd").parse(when)
            val whenFormatted = new SimpleDateFormat("yyyy-MM-dd").format(wdt)
            freshRows(df.filter(s"$ts <= '$whenFormatted'")) //.sort(keyFields.get.head, keyFields.get.tail:_*)
          }
          case None => df
        }
        val limited = limited1.checkpoint
        log.warn(s"getNew: freshRows: monthly count up to $when is ${limited.count} new rows of $name")

        val newDF1 = fullDF match { // FIXME: (aaa) was monthlyDF, but it misses rows read during loadInitial
          case Some(prev) => {

            // get new rows not in fullDF yet
            // limited.except(prev)

            // get new rows not in fullDF yet
            // ignore any timstamp columns
            doExcept(limited, prev)

            // val thisSrcCols = if (srcCols.isEmpty) prev.schema.fieldNames.toList else srcCols
            // val srcColsWithoutTS = getColsExcludeTS(thisSrcCols)
            // // use leftanti join to get all new rows that have changed excluding timestamp columns
            // // use a null safe join operator
            // val nullSafeJoinConditions = srcColsWithoutTS.map(colName => {
            //   limited(colName) <=> prev(colName)
            // }).reduce( _ and _ )
            // limited.join(prev, nullSafeJoinConditions, "leftanti")

          }
          case None => limited
        }

        val newDF = newDF1.checkpoint
        latestDF.foreach(old => old.unpersist)
        latestDF = Some(newDF)

        log.warn(s"getNew: found ${newDF.count} rows of $name")
        //newDF.show(20)

        if (newDF.count > 0) {
          val written = writer match {
            case Some(wfunc) => {
              log.warn(s"getNew: going to write ${newDF.count} rows of $name")
              wfunc(newDF, this.name)
            }
            case None => 0
          }

          monthlyDF.foreach(old => old.unpersist)
          monthlyDF = Some(limited)

          val newFull1 = destTimestamp match {
            case Some(x) => {
              // check if fullDF exists
              fullDF match {
                case Some(x) => freshRows(if (x.count > 0) doUnion(x, newDF) else newDF)
                // .sort(keyFields.get.head, keyFields.get.tail:_*)
                case None => freshRows(newDF)
              }
            }
            case None => {
              // check if fullDF exists
              fullDF match {
                case Some(x) => doUnion(x, newDF).dropDuplicates
                case None => newDF.dropDuplicates
              }
            }
          }

          //val newFull = setDFName(newFull1, s"newFull_$yymm").persist//newFull1.persist
          val newFull = newFull1.checkpoint //persist

          log.warn(s"now holding ${newFull.count} rows of $name")
          newFull.show(20)

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

    log.warn(s"loadInitial $startYY $startMM thru $endYY $endMM")

    while (f"$startYY${startMM}%02d" <= f"$endYY${endMM}%02d") {

      val fullPath = f"${path}/${startYY}/${startMM}%02d"

      var start = System.currentTimeMillis()

      log.warn(s"loading $fullPath")

      val newDF = loadDF(fullPath)

      newDF match {
        case Some(df) => {
          // df.persist
          // df.show(50)
          val fDf = if (startYY == endYY && startMM == endMM && endMonth.length > 6) {
            destTimestamp match {
              case Some(ts) => {
                val edt = new SimpleDateFormat("yyyyMMdd").parse(endMonth)
                val endMonthFormatted = new SimpleDateFormat("yyyy-MM-dd").format(edt)
                freshRows(df.filter(s"$ts <= '$endMonthFormatted'"))
              }
              case None => df
            }
          } else {
            df
          }

          val newFull = fullDF match {
            case Some(x) => doUnion(x, fDf)
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

    if (!fullDF.isEmpty) {

      val preReduce = fullDF.get
      log.warn(s"loading freshRows for ${this.name}, count: " + preReduce.count())

      var start = System.currentTimeMillis()
      val postReduce = freshRows(preReduce).persist

      log.warn(s"done loading freshRows ${postReduce.count} rows for ${this.name}")

      var duration = System.currentTimeMillis() - start
      fullDF.foreach(old => old.unpersist)
      fullDF = Some(postReduce)

    }

    fullDF
  }

  override def all() = {
    // println("TimePartitionedCsvTable: all() start...")
    lastNewOption = None

    // months ago
    val monthsAgo = 12

    // start from months ago
    val start = LocalDate.now.minusMonths(monthsAgo)
      .`with`(TemporalAdjusters.firstDayOfMonth)
      .toString.replace("-", "")
      .substring(0, 6)

    // last day of current month
    val end = LocalDate.now
      .`with`(TemporalAdjusters.lastDayOfMonth)
      .toString.replace("-", "")

    // println(s"TimePartitionedCsvTable: all(): apiTest: $apiTest")

    // run once or test mode
    val returnDF = loadInitial(start, end)

    // println("TimePartitionedCsvTable: all() done.")

    returnDF
  }

  override def initial(since: String, till: String) = {
    // batch mode
    val initFullDF = readInitFullDF(readInitPath)
    initFullDF match {
      case Some(x) => {
        // we read fullDF from path, return None to prevent writing again
        None
      }
      case None => loadInitial(since, till)
    }
  }

  override def newest(till: String) = {

    // FIXME: (aaa) is this required, otherwise this will keep returning
    // the last newest() rows till we catchUp to current date

    // val nextNewDF = lastNewOption match {
    //   case Some(x) => x
    //   case None => getNew(till)
    // }
    // lastNewOption = Some(nextNewDF)

    val nextNewDF = getNew(till)
    nextNewDF
  }
}


object CustomUDF {

  case class DerivedTSWithFormatError(formatted: String, formatError: String);
  val toDerivedTSSchema = new StructType()
    .add("DERIVED_TS", StringType)
    .add("PARSE_ERROR", StringType)

  /* parse zoned date time */
  private def parseZonedDateTime(ts: String, tsFormat: String): Try[ZonedDateTime] = {
    Try {
      val formatter = DateTimeFormatter.ofPattern(tsFormat)
      // try zoned date time
      ZonedDateTime.parse(ts, formatter)
    } match {
      case Success(x) => Success(x)
      case Failure(ex) => {
        /* parse DateTimeFormatter.ISO_ZONED_DATE_TIME
         2007-12-03T10:15:30+01:00[Europe/Paris] */
        Try { ZonedDateTime.parse(ts) }
      }
    }
  }

  /* parse date time */
  private def parseDateTime(ts: String, tsFormat: String, tsTimezone: String): Try[ZonedDateTime] = {
    Try {
      val formatter = DateTimeFormatter.ofPattern(tsFormat)
      // try zoned date time
      LocalDateTime.parse(ts, formatter).atZone(ZoneId.of(tsTimezone, ZoneId.SHORT_IDS))
    } match {
      case Success(x) => Success(x)
      case Failure(ex) => {
        /* parse DateFormatter.ISO_DATE_TIME
         2007-12-03T10:15:30 */
        Try { LocalDateTime.parse(ts).atZone(ZoneId.of(tsTimezone, ZoneId.SHORT_IDS)) }
      }
    }
  }

  /* parse date time */
  private def parseDate(ts: String, tsFormat: String, tsTimezone: String): Try[ZonedDateTime] = {
    Try {
      val formatter = DateTimeFormatter.ofPattern(tsFormat)
      // try date formatter
      LocalDate.parse(ts, formatter).atStartOfDay().atZone(ZoneId.of(tsTimezone, ZoneId.SHORT_IDS))
    } match {
      case Success(x) => Success(x)
      case Failure(ex) => {
        /* parse
         DateFormatter.ISO_LOCAL_DATE 2007-12-03 */
        Try { LocalDate.parse(ts).atStartOfDay().atZone(ZoneId.of(tsTimezone, ZoneId.SHORT_IDS)) }
      }
    }
  }

  /** toDerivedTS udf - parses timestamp string according to specified
    * pattern and timezone (when timezone info is unavailable in the timestamp string)
    * and outputs the timestamp in standard "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" format after converting to UTC.
    */
  def toDerivedTS = udf((ts: String, tsFormat:String, tsTimezone: String) => {

    val destFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

    val converted =
      // try zoned date time
      parseZonedDateTime(ts, tsFormat) match {
        case Success(x) => Some(x)
        case Failure(ex) => {
          // try local date time
          parseDateTime(ts, tsFormat, tsTimezone) match {
            case Success(x) => Some(x)
            case Failure(ex) => {
              // try local date
              parseDate(ts, tsFormat, tsTimezone) match {
                case Success(x) => Some(x)
                case Failure(ex) => None
              }
            }
          }
        }
      }
    // convert ZonedDateTime to UTC and format to derived ts format
    val derived = converted match {
      case Some(x) => DerivedTSWithFormatError(x.withZoneSameInstant(ZoneId.of("UTC")).format(destFormat), "")
      case None => DerivedTSWithFormatError(null, s"DATE FORMAT ERROR $ts cannot be formatted with pattern $tsFormat and timezone $tsTimezone")
    }

    derived

  }, toDerivedTSSchema) //end of udf

}


class DQException(message: String, cause: Throwable)
    extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}

class DQDerivedTSFormatException(message: String, cause: Throwable)
    extends DQException(message, cause) {
  def this(message: String) = this(message, null)
}


class DQConfigException(message: String, cause: Throwable)
    extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}


class DQDataException(message: String, cause: Throwable)
    extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}


class DQCheck(val spark: org.apache.spark.sql.SparkSession,
  val readOptions: Map[String, String],
  val table: Table) {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  def readCSVFile(filePath: String, srcCols: List[String]): Try[DataFrame] =  Try {
    //try {
    val dfReader = if (srcCols.isEmpty) {
      spark.read.format("csv").options(readOptions)
    } else {
      val customSchema = StructType(srcCols.map(fieldName => StructField(fieldName.trim, StringType, true)))
      spark.read.format("csv").options(readOptions).schema(customSchema)
    }
    dfReader.load(filePath)
    // } catch {
    //   case ex: Throwable => {
    //     throw new DQDataException(s"Error reading file $filePath", ex)
    //   }
    // }
  }

  def checkDQKeyCols(df: DataFrame, filePath: String, keyFields: Option[List[String]]): Try[DataFrame] = {
    // log.warn("checkKeyCols: start")
    // check for keyCols in the df header's
    val dfColumns = df.columns.toList

    if (keyFields.isEmpty || keyFields.get.isEmpty) {
      //val runtimeApplicationId = spark.sparkContext.applicationId
      val message = s"""key fields have not been specified"""
        .stripMargin.replaceAll("\n", " ")
      Failure(new DQConfigException(message))
    } else if (!keyFields.get.forall(dfColumns.contains)) {
      //val runtimeApplicationId = spark.sparkContext.applicationId
      val message = s"""file $filePath with columns ${dfColumns.mkString(",")}
              | has missing key columns: ${keyFields.get.mkString(",")}"""
        .stripMargin.replaceAll("\n", " ")
      Failure(new DQException(message))
    } else {
      // return the valid df
      Success(df)
    }
  }

  def checkDQDuplicateCols(df: DataFrame, filePath: String): Try[DataFrame] = {

    // log.warn("checkDuplicateCols: start")

    // remove check where to ignore trailing numbers when
    // check for duplicates because we cannot distinguish between
    // column names that have trailing numbers by design or because the csv had
    // duplicate  column names

    // val colDigit = raw"([^0-9]*)([0-9]*)".r
    // check for keyCols in the df header's
    //val dfColumns = df.columns.toList.map(x => s"$x" match {case colDigit(prefix, num) => s"$prefix"})
    val dfColumns = df.columns.toList

    val dupHeaders = dfColumns.groupBy(identity).collect{case (x, ys) if ys.lengthCompare(1) > 0 => x }
    if (dupHeaders.toList.length > 0) {
      val message = s"""file $filePath
              | found duplicate columns: ${dfColumns.mkString(",")}"""
        .stripMargin.replaceAll("\n", " ")
      Failure(new DQException(message))
    } else {
      // return the valid df
      Success(df)
    }
  }

  def checkDQDerivedTS(df: DataFrame,
    filePath: String,
    filename: String,
    srcTimestamp: Option[List[String]],
    srcTimestampFormat: Option[String],
    srcTimezone: Option[String],
    destTimestampFormat: String,
    lastModified: String): Try[DataFrame] = {

    var derivedTS = df
    // add filename and lastModified
    derivedTS = derivedTS.withColumn("FILENAME_TS", lit(filename))
      .withColumn("LAST_MODIFIED_TS", lit(lastModified))

    // get original columns of the data frame
    val originalCols = derivedTS.schema.fieldNames.toList

    // extract timestamp fields from the dataframe
    if (srcTimestamp.isEmpty) {

      val message = s"""file $filePath
              | srcTimestamp is not specified"""
        .stripMargin.replaceAll("\n", " ")
      Failure(new DQConfigException(message))

    } else if (!srcTimestampFormat.isEmpty && srcTimestampFormat.get.trim == "") {

      val message = s"""file $filePath
              | srcTimestampFormat is empty"""
        .stripMargin.replaceAll("\n", " ")
      Failure(new DQConfigException(message))

    } else if (!srcTimestamp.get.forall(derivedTS.columns.toList.contains)) {

      val message = s"""file $filePath with columns ${derivedTS.columns.mkString(",")}
              | does not contain srcTimestamp fields ${srcTimestamp.get.mkString(",")}"""
        .stripMargin.replaceAll("\n", " ")
      Failure(new DQConfigException(message))

    } else if (!srcTimezone.isEmpty && srcTimezone.get.trim == "") {

      val message = s"""file $filePath
              | srcTimezone is not specified"""
        .stripMargin.replaceAll("\n", " ")
      Failure(new DQConfigException(message))

    } else if (!TimeZone.getAvailableIDs.contains(srcTimezone.get)) {

      val message = s"""file $filePath
              | srcTimezone ${srcTimezone.get} is not valid"""
        .stripMargin.replaceAll("\n", " ")
      Failure(new DQConfigException(message))

    } else {

      val dcols = srcTimestamp.get
      derivedTS = derivedTS.withColumn("DERIVED_TS", lit(""))
      dcols.map(x => {
        derivedTS = derivedTS.withColumn("DERIVED_TS", concat(
          when(col("DERIVED_TS").isNotNull, col("DERIVED_TS")).otherwise(lit("")),
          lit(" "),
          when(col(x).isNotNull, col(x)).otherwise(lit(""))))
      })

      val formattedDFOut = derivedTS.withColumn("toTSOut",
        CustomUDF.toDerivedTS(trim(col("DERIVED_TS")), lit(srcTimestampFormat.get), lit(srcTimezone.get)))

      val selectCols = originalCols ++ List("toTSOut.DERIVED_TS", "toTSOut.PARSE_ERROR")

      // select original columns
      val formattedDF = formattedDFOut.select(selectCols.head, selectCols.tail:_*)

      // select rows where DERIVED_TS exists
      val formattedDFNull = formattedDF.filter(col("DERIVED_TS").isNull)

      if (formattedDFNull.count() > 0) {

        val message = s"""file $filePath cannot compute timestamp from srcTimestamp fields ${dcols.mkString(",")}
           | with srcTimestampFormat ${srcTimestampFormat.get} and srcTimezone ${srcTimezone.get} for ${formattedDFNull.count()} rows"""
          .stripMargin.replaceAll("\n", " ")

        Failure(new DQDerivedTSFormatException(message))

      } else {

        Success(df)

      }
    }
  }

  /** Returns a dataframe if all data quality checks pass
    */
  def checkFileDQ(filePath: String, filesFormat: String): Try[DataFrame]  = {

    val fileStatus = HdfsUtils.getFileStatus(filePath).get
    val filename = fileStatus.getPath.getName
    val lastModified = fileStatus.getModificationTime
    val lastModifiedFormatted =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(lastModified)

    filesFormat match {
      case "CSV" => {
        val preprocessor = table.asInstanceOf[TimePartitionedCsvTable]
        for {
          df  <- readCSVFile(filePath, preprocessor.srcCols) // map
          df1 <- checkDQKeyCols(df, filePath, preprocessor.keyFields)
          df2 <- checkDQDuplicateCols(df, filePath)
          df3 <- checkDQDerivedTS(df,
            filePath,
            filename,
            preprocessor.srcTimestamp,
            preprocessor.srcTimestampFormat,
            preprocessor.srcTimezone,
            preprocessor.destTimestampFormat,
            lastModifiedFormatted)
        } yield df
      }
      case _ => {
        val message = s"""preprocessor only supports CSV format"""
        Failure(new DQConfigException(message))
      }
    }

    // val runtimeApplicationId = spark.sparkContext.applicationId
    // val exMesg = ex.getMessage
    // val exStackTrace = ex.getStackTrace
    // log.error(s"""$runtimeApplicationId - READ_RAW_CSV_ERROR -
    //       |applicationId: 1, appName: flow, appType: flow,
    //       |exceptionMessage: $exMesg, exceptionTrace: $exMesg,
    //       |message: error reading raw file $filePath with exception $exMesg"""
    //   .stripMargin.replaceAll("\n", " "))
    // None
    //}
  }

}
