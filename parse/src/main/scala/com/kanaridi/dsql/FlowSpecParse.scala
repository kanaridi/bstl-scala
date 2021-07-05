/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import java.io.InputStream
import java.util.Map

import scala.io.Source
import scala.util.parsing.json.JSONObject

import com.fasterxml.jackson.databind.ObjectMapper

import com.kanaridi.bstl.JsonPath
import com.kanaridi.common.util.HdfsUtils
import com.kanaridi.common.util.TimeUtils
import com.kanaridi.xform.JObjExplorer

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**  returns a flow operator object model by processing the JSON spec for that model
  */
class FlowSpecParse(json: Map[String, Any]) extends JObjExplorer with Serializable {

  val ast: Map[String, Any] = json
  val log: Log = LogFactory.getLog(FlowSpecParse.getClass.getName)

  // remembers any "tee" operators we may be building
  val teeDict = new scala.collection.mutable.HashMap[String, TeeOp]()
  val splitDict = new scala.collection.mutable.HashMap[String, SplitOp]()

  // signalId -> upstream operators dictionary
  val usfoDict = new scala.collection.mutable.HashMap[String, Table]()

  // signalId -> downstream operators dictionary
  val dsfoDict = new scala.collection.mutable.HashMap[String, Table]()

  // a hashtable of functions that configure and return curried functions, ready to write dataframes to a particular target
  val connectionWriterFactories = StandardWriterFactories()

  var previewMode = false
  def setPreviewMode: Unit = {
    previewMode = true
  }

  var apiTest: Boolean = false
  // to disable all writing, clear out all our writers, leaving only /dev/null
  //  useful for developer mode
  def disableWrites: Unit = {
    connectionWriterFactories.clear
    apiTest = true
  }

  var embargo: Boolean = true
  def disableEmbargo: Unit = {
    embargo = false
  }

  def getPipelineId(): String = {
    stringVal(ast, "id", "none")
  }

  def pipelineOptions(): Any = {
    val q = "$.options"
    val so = subObjects(q, ast)
    log.warn(s"pipelineOptions $q are ${so(0)}")
    so(0)
  }

  def getCheckpointDir(): String = {
    stringVal(pipelineOptions, "options.checkpoint", "checkpoint")
  }

  def flowOpName(flow: Any) = {
    stringVal(flow, "signals.sources[0].signalId", "anonymous")
  }

  def flowOpType(flow: Any) = {
    stringVal(flow, "operatorType", "t?")
  }

  // look downstream to find the farthest downstream node, returning the signal name leading into it
  def findFurthestSignal(upstreamName: String, primaryPath: List[String]): String = {
    val q = "$.operations[*][?(@.signals.sources[?(@.signalId == '" + upstreamName + "')])]"
    val so = subObjects(q, ast)
    // println(s"downstream flow $q is $so")
    if (so.length == 0)
      upstreamName
    else {
      val nextUpstream = stringVal(so(0), "signals.sinks[0].signalId", "-none-")
      if (primaryPath.contains(flowOpName(so(0))))
        "-skippable-"
      else if (nextUpstream == "-none-")
        upstreamName
      else
        findFurthestSignal(stringVal(so(0), "signals.sinks[0].signalId", ""), primaryPath)
    }
  }

  def flowOpFromJobj(flow: Any, sigName: String, spark: org.apache.spark.sql.SparkSession, path: List[String] = List[String]()): Option[Table] = {

    // flow is jackson parser's instantiation of a JSON object
    // sigName is the name of the signal we folloewd to get to this object

    val sourceNames = vectorVal(flow, "signals.sources[*].signalId").toList.map(x => ("" + x))
    val sinkNames = vectorVal(flow, "signals.sinks[*].signalId").toList.map(x => ("" + x))
    val foName = flowOpName(flow) // stringVal(flow, "signals.sinks[0].signalId", "anonymous")
    val foType = flowOpType(flow) // stringVal(flow, "operatorType", "t?")

    val sources = sourceNames.map(x => {
      x match {
        case name: String => getUpstreamOp(name, spark, foName :: path)
      }
    })


    def makeWriter(): Option[Table] = {
      // println(s"FlowSpecParse: building a Sink $foName")
      val from = stringVal(flow, "options.prefetch.from", "")
      val to = stringVal(flow, "options.prefetch.to", "")
      val save = (stringVal(flow, "options.prefetch.save", "false") == "true")
      val catchupIncrement = stringVal(flow, "options.catchupIncrement", "1 day")
      val refreshInterval = TimeUtils.timeStringAsMs(stringVal(flow, "options.refreshInterval", "15 minutes"))
      val topic = stringVal(flow, "options.topic", "order_details")
      val sink = new Sink(spark, foName, sources(0).get, from, to, save, catchupIncrement, refreshInterval)
      val connId = stringVal(flow, "signals.sinks[0].connectionId", "NULL")
      if (connId != "NULL") {
        val connection = {
          // FIXME: ast
          val q = "$.connections[?(@.id == '" + connId + "')]"
          val so = subObjects(q, ast)
          log.warn(s"connection $q is ${so(0)}")
          so(0)
        }
        val connectionType = stringVal(connection, "connectionType", "local")
        // println(s"FlowSpecParse: getting writer factory for $connectionType")
        val factory = connectionWriterFactories.getOrElse(connectionType, StandardWriterFactories.nullFactory(_,_,_))
        // println(s"FlowSpecParse: got  factory of $factory")
        val writer = factory(connectionType, flow, connection)
        // println(s"FlowSpecParse: got writer of $writer")
        sink.send = writer
      }
      Some(sink)
    }

    val fo = foType match {

      case "tee" => {
        val sinkId1 = sinkNames(0)

        val teeOp = teeDict.get(foName) match {
          case Some(firstTee) => {
            new TeeOpSecondSink(firstTee)
          }
          case None => {
            val tMaster = new TeeOp(sources(0).get)
            teeDict.put(foName, tMaster)


            // look downstream to find the farthest node
            def findFirst(upstreamName: String): String = {
              val q = "$.operations[*][?(@.signals.sources[?(@.signalId == '" + upstreamName + "')])]"
              val so = subObjects(q, ast)
              // println(s"downstream flow $q is $so")
              if (so.length == 0)
                upstreamName
              else {
                val nextUpstream = stringVal(so(0), "signals.sinks[0].signalId", "-none-")
                if (nextUpstream == "-none-")
                  upstreamName
                else
                  findFirst(stringVal(so(0), "signals.sinks[0].signalId", ""))
              }
            }

            tMaster.alternates = if (apiTest)
              Some(List[Table]())
            else
              Some(sinkNames.filter( x => x != sigName).map( x =>  getDownstreamOp(findFurthestSignal(x, path), spark)).flatten)

            tMaster

          }
        }

        Some(teeOp)
      }


      case "split" => {
        val sinkId1 = sinkNames(0)
        val foKey = stringVal(flow, "signals.sources[0].signalId", "anonymous")

        val splitOp = splitDict.get(foKey) match {
          case Some(firstSplit) => {
            new SplitOpSecondSink(firstSplit)
          }
          case None => {
            val expr = stringVal(flow, "options.expr", "1")
            val tMaster = new SplitOp(sources(0).get, expr)
            splitDict.put(foKey, tMaster)

            def findFirst(upstreamName: String): String = {
              val q = "$.operations[*][?(@.signals.sources[?(@.signalId == '" + upstreamName + "')])]"
              val so = subObjects(q, ast)
              // println(s"downstream flow $q is $so")
              if (so.length == 0)
                upstreamName
              else {
                val nextUpstream = stringVal(so(0), "signals.sinks[0].signalId", "-none-")
                if (nextUpstream == "-none-")
                  upstreamName
                else
                  findFirst(stringVal(so(0), "signals.sinks[0].signalId", ""))
              }
            }

            tMaster.alternates = Some(sinkNames.filter( x => x != sigName).map( x =>  getDownstreamOp(findFirst(x), spark)).flatten)

            tMaster

          }
        }

        Some(splitOp)
      }

      // converts a join outcome to timestamped events
      case "joinToEvents" => {
        // val sources = vectorVal(flow, "signals.sources[*].signalId").toList.map(x => {
        //   x match {
        //     case name: String => getUpstreamOp(name, spark)
        //   }
        // })
        val cols = vectorVal(flow, "options.cols").toList.map(x => ("" + x))
        val left = sources(0)
        val right = sources(1)
        val joinType = stringVal(flow, "options.joinType", "inner")
        val joinOp = new JoinOp(left.get, right.get, cols, joinType)

        val keyCols = vectorVal(flow, "options.keyCols").toList.map(x => ("" + x))
        val eventTimestamp = stringVal(flow, "options.eventTimestamp", "")
        val leftTimestamp = stringVal(flow, "options.timestamp_a", "")
        val rightTimestamp = stringVal(flow, "options.timestamp_b", "")
        val keepTimestamps = stringVal(flow, "options.keepTimestamps", "true")
        val toEvents = new ToEventsOp(joinOp, keyCols, leftTimestamp, rightTimestamp, eventTimestamp)
        Some(toEvents)
      }

      case "toEvents" => {
        val keyCols = vectorVal(flow, "options.keyCols").toList.map(x => ("" + x))
        val eventTimestamp = stringVal(flow, "options.eventTimestamp", "")
        val leftTimestamp = stringVal(flow, "options.timestamp_a", "")
        val rightTimestamp = stringVal(flow, "options.timestamp_b", "")
        val toEvents = new ToEventsOp(sources(0).get, keyCols, leftTimestamp, rightTimestamp, eventTimestamp)
        Some(toEvents)
      }

      case "deDup" => {
        val eventTimestamp = stringVal(flow, "options.eventTimestamp", "")
        val ignorableCols = vectorVal(flow, "options.ignorableCols").toList.map(x => ("" + x))
        val op = new DeDupOp(sources(0).get, eventTimestamp, ignorableCols)
        Some(op)
      }

      // fixme
      case "partitionedAvroSource" => {
        val srcCols = vectorVal(flow, "options.srcCols").toList.map(x => ("" + x))
        val cols = vectorVal(flow, "options.cols").toList.map(x => ("" + x))
        val keyCols = vectorVal(flow, "options.keyCols").toList.map(x => ("" + x))
        val filePath = stringVal(flow, "options.filePath", "---")
        val srcTimestamp = stringVal(flow, "options.srcTimestamp", "")
        val timestamp = stringVal(flow, "options.timestamp", "")
        val filter = stringVal(flow, "options.filter", "")
        val fromMonth = stringVal(flow, "options.frommonth", "")
        // println(s"flow $foName has srcCols $srcCols")
        val tpat = new TimePartitionedAvroTable(
          spark,
          foName,
          filePath,
          srcCols,
          cols,
          if (srcTimestamp.length > 0) Some(srcTimestamp) else None,
          if (timestamp.length > 0) Some(timestamp) else None,
          if (keyCols.length > 0) Some(keyCols) else None,
          if (filter.length > 0) Some(filter) else None,
          if (fromMonth.length > 0) Some(filter) else None
        )
        Some(tpat)
      }

      // FIXME
      case "simpleJsonResource" => {
        val filePath = stringVal(flow, "options.filePath", "---")
        val tpat = new SimpleJsonResource(spark, foName, filePath)
        Some(tpat)
      }

      case "simpleJsonFile" => {
        val connection = {
          val signalId = stringVal(flow, "signals.sinks[0].signalId", "none")

          // FIXME: ast
          val q = "$.connections[*][?(@connectionID == '" + signalId + "')]"
          val so = subObjects(q, ast)
          log.info(s"connection $q is $so")
          so(0)
        }

        val filePath = stringVal(connection, "connection.connectionStrings.url", "hdfs://") +
        stringVal(connection, "options.path", "/kanari/data/wom-uss")
        val tpat = new SimpleJsonFile(spark, foName, filePath)
        Some(tpat)
      }

      case "preprocessor" => {

        //get operator options
        val readInitPath = stringVal(flow, "options.cleanDataPath", "/dev/null")
        val since = stringVal(flow, "options.since", "")
        val srcCols = vectorVal(flow, "options.srcCols").toList.map(x => ("" + x))
        val cols = vectorVal(flow, "options.cols").toList.map(x => ("" + x))
        val keyCols = vectorVal(flow, "options.keyCols").toList.map(x => ("" + x))
        val timestamp = stringVal(flow, "options.timestamp", "DERIVED_TS")
        val srcTimestamp = vectorVal(flow, "options.srcTimestamp").toList.map(x => ("" + x))
        val srcTimestampFormat = stringVal(flow, "options.srcTimestampFormat", " ")
        val srcTimezone = stringVal(flow, "options.srcTimezone", "UTC")

        // get connection
        val connection = {
          val signalId = stringVal(flow, "signals.sources[0].connectionId", "none")
          val q = "$.connections[?(@.id == '" + signalId + "')]"
          val so = subObjects(q, ast)
          log.warn(s"connection $q is ${so(0)}")
          so(0)
        }

        val connectionType = stringVal(connection, "connectionType", "local")
        connectionType match {
          case "blob-storage" | "hdfs" | "blob-storage-stream" | "blob-storage-read" | "hdfs-stream" | "hdfs-read" => {

            val filePath = connectionType match {

              case "blob-storage" => {
                val pathConn = stringVal(connection, "options.path", "")
                val pathFlow = stringVal(flow, "options.path", "")
                val path = (
                  if (pathConn != "" && pathFlow != "") HdfsUtils.resolvePaths(pathConn, pathFlow).toString
                  else if (pathConn != "" && pathFlow == "") HdfsUtils.resolvePaths(pathConn).toString
                  else if (pathConn == "" && pathFlow != "") HdfsUtils.resolvePaths(pathFlow).toString
                  else "/dev/null"
                )
                "abfs://" + stringVal(connection, "connection.connectionStrings.container", "none") + "@" +
                stringVal(connection, "connection.connectionStrings.storageAccount", "nobody.dfs.windows.net") +
                path
              }
              case "blob-storage-stream" | "blob-storage-read" => { // deprecated connection types
                "abfs://" + stringVal(connection, "connection.connectionStrings.container", "none") + "@" +
                stringVal(connection, "connection.connectionStrings.storageAccount", "nobody.dfs.windows.net") +
                stringVal(connection, "options.path", "/dev/null")
              }
              case "hdfs" => {
                val pathConn = stringVal(connection, "options.path", "")
                val pathFlow = stringVal(flow, "options.path", "")
                val path = (
                  if (pathConn != "" && pathFlow != "") HdfsUtils.resolvePaths(pathConn, pathFlow).toString
                  else if (pathConn != "" && pathFlow == "") HdfsUtils.resolvePaths(pathConn).toString
                  else if (pathConn == "" && pathFlow != "") HdfsUtils.resolvePaths(pathFlow).toString
                  else "/dev/null"
                )
                "hdfs://" +  path
              }
              case "hdfs-stream" | "hdfs-read" => { // deprecated connection types
                "hdfs://" +  stringVal(connection, "options.path", "/dev/null")
              }
            }

            val filesFormatConn = stringVal(connection, "options.filesFormat", "")
            val filesFormatFlow = stringVal(flow, "options.filesFormat", "")
            val filesFormat = (
              if (filesFormatFlow != "") filesFormatFlow
              else if (filesFormatConn != "") filesFormatConn
              else "CSV"
            )

            filesFormat match {
              case "CSV" => {

                val headerConn = stringVal(connection, "options.header", "")
                val headerFlow = stringVal(flow, "options.header", "")
                val header = (
                  if (headerFlow != "") headerFlow
                  else if (headerConn != "") headerConn
                  else "true"
                )

                val filesFormatCsvSeperatorConn = stringVal(connection, "options.filesFormatCsvSeperator", "")
                val filesFormatCsvSeperatorFlow = stringVal(flow, "options.filesFormatCsvSeperator", "")
                val filesFormatCsvSeparator = (
                  if (filesFormatCsvSeperatorFlow != "") filesFormatCsvSeperatorFlow
                  else if (filesFormatCsvSeperatorConn != "") filesFormatCsvSeperatorConn
                  else ","
                )

                val inferSchemaConn = stringVal(connection, "options.inferSchema", "")
                val inferSchemaFlow = stringVal(flow, "options.inferSchema", "")
                val inferSchema = (
                  if (inferSchemaFlow != "") inferSchemaFlow
                  else if (inferSchemaConn != "") inferSchemaConn
                  else "false" // for preprocessor only
                )

                // println(s"connection $foName has srcCols $srcCols")
                val tpat = new TimePartitionedCsvTable(
                  spark,
                  foName,
                  filePath,
                  srcCols,
                  cols,
                  if (srcTimestamp.length > 0) Some(srcTimestamp) else None,
                  if (timestamp.length > 0) Some(timestamp) else None,
                  if (keyCols.length > 0) Some(keyCols) else None,
                  if (since.length > 0) Some(since) else None,
                  if (srcTimestampFormat.length > 0) Some(srcTimestampFormat) else None,
                  if (srcTimezone.length > 0) Some(srcTimezone) else None,
                  if (readInitPath.length > 0) Some(readInitPath) else None,
                  if (header.length > 0) Some(header) else None,
                  if (filesFormatCsvSeparator.length > 0) Some(filesFormatCsvSeparator) else None,
                  if (inferSchema.length > 0) Some(inferSchema) else None,
                  apiTest,
                  previewMode,
                  embargo
                )
                Some(tpat)
              }
              case _ => {
                val errorMsg = "preprocessor only supports connection.options.filesFormat: 'CSV'"
                log.error(errorMsg)
                // throw new Exception(errorMsg)
                None
              }
            }
          }
          case _ => {
            val errorMsg = "preprocessor only supports blob-storage or blob-storage-stream or hdfs-storage-stream connection type"
            log.error(errorMsg)
            // throw new Exception(errorMsg)
            None
          }
        }
      }
      case "reader" => {
        val connection = {
          val signalId = stringVal(flow, "signals.sources[0].connectionId", "none")
          val q = "$.connections[?(@.id == '" + signalId + "')]"
          val so = subObjects(q, ast)
          log.warn(s"connection $q is ${so(0)}")
          so(0)
        }

        val connectionType = stringVal(connection, "connectionType", "local")

        connectionType match {
          case "jdbc" => {

            val sqlUser = stringVal(connection, "connection.security.userNameAndPassword.userName", "nobody")
            val sqlPass = stringVal(connection, "connection.security.userNameAndPassword.password", "nobody")
            val sqlUrl = stringVal(connection, "connection.connectionStrings.url", "")
            val driver = stringVal(connection, "connection.connectionStrings.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

            val sqlTable = stringVal(flow, "options.sqlTable", "")

            Some(new JdbcTable(spark, sqlUrl, sqlUser, sqlPass, driver, Some(sqlTable), None))
          }

          case "event-hub" | "event-hub-read" => {
            val namespace = stringVal(connection, "connection.connectionStrings.namespace", "nobody")
            val name = stringVal(connection, "connection.connectionStrings.entityName", "nobody")
            val sasKey = stringVal(connection, "connection.connectionStrings.sharedAccessKey", "nobody")
            val sasKeyName = stringVal(connection, "connection.connectionStrings.sharedAccessKeyName", "nobody")

            var options: scala.collection.immutable.Map[String, String] = scala.collection.immutable.Map[String, String]()

            // get options
            val autoOffsetResetConn = stringVal(connection, "options.autoOffsetReset", "")
            val autoOffsetResetFlow = stringVal(flow, "options.autoOffsetReset", "")
            options = options + ("autoOffsetReset" -> (
              if (autoOffsetResetFlow != "") autoOffsetResetFlow
              else if (autoOffsetResetConn != "") autoOffsetResetConn
              else "earliest")
            )

            val consumerGroupConn = stringVal(connection, "options.consumerGroup", "")
            val consumerGroupFlow = stringVal(flow, "options.consumerGroup", "")
            options = options + ("consumerGroup" -> (
              if (consumerGroupFlow != "") consumerGroupFlow
              else if (consumerGroupConn != "") consumerGroupConn
              else "notset")
            )

            val maxRatePerPartitionConn = stringVal(connection, "options.maxRatePerPartition", "")
            val maxRatePerPartitionFlow = stringVal(flow, "options.maxRatePerPartition", "")
            options = options + ("maxRatePerPartition" -> (
              if (maxRatePerPartitionFlow != "") maxRatePerPartitionFlow
              else if (maxRatePerPartitionConn != "") maxRatePerPartitionConn
              else "5000")
            )

            val operationTimeoutConn = stringVal(connection, "options.operationTimeout", "")
            val operationTimeoutFlow = stringVal(flow, "options.operationTimeout", "")
            options = options + ("operationTimeout" -> (
              if (operationTimeoutFlow != "") operationTimeoutFlow
              else if (operationTimeoutConn != "") operationTimeoutConn
              else "60")
            )

            val receiverTimeoutConn = stringVal(connection, "options.receiverTimeout", "")
            val receiverTimeoutFlow = stringVal(flow, "options.receiverTimeout", "")
            options = options + ("receiverTimeout" -> (
              if (receiverTimeoutFlow != "") receiverTimeoutFlow
              else if (receiverTimeoutConn != "") receiverTimeoutConn
              else "60")
            )

            val ehdf = new EventhubTable(spark, namespace, name, sasKeyName, sasKey, options)

            Some(ehdf)

          }
          case "blob-storage" | "hdfs" | "blob-storage-stream" | "blob-storage-read" | "hdfs-stream" | "hdfs-read" => {

            val filePath = connectionType match {

              case "blob-storage" => {
                val pathConn = stringVal(connection, "options.path", "")
                val pathFlow = stringVal(flow, "options.path", "")
                val path = (
                  if (pathConn != "" && pathFlow != "") HdfsUtils.resolvePaths(pathConn, pathFlow).toString
                  else if (pathConn != "" && pathFlow == "") HdfsUtils.resolvePaths(pathConn).toString
                  else if (pathConn == "" && pathFlow != "") HdfsUtils.resolvePaths(pathFlow).toString
                  else "/dev/null"
                )
                "abfs://" + stringVal(connection, "connection.connectionStrings.container", "none") + "@" +
                stringVal(connection, "connection.connectionStrings.storageAccount", "nobody.dfs.windows.net") +
                path
              }
              case "blob-storage-stream" | "blob-storage-read" => { // deprecated connection types
                "abfs://" + stringVal(connection, "connection.connectionStrings.container", "none") + "@" +
                stringVal(connection, "connection.connectionStrings.storageAccount", "nobody.dfs.windows.net") +
                stringVal(connection, "options.path", "/dev/null")
              }
              case "hdfs" => {
                val pathConn = stringVal(connection, "options.path", "")
                val pathFlow = stringVal(flow, "options.path", "")
                val path = (
                  if (pathConn != "" && pathFlow != "") HdfsUtils.resolvePaths(pathConn, pathFlow).toString
                  else if (pathConn != "" && pathFlow == "") HdfsUtils.resolvePaths(pathConn).toString
                  else if (pathConn == "" && pathFlow != "") HdfsUtils.resolvePaths(pathFlow).toString
                  else "/dev/null"
                )
                "hdfs://" +  path
              }
              case "hdfs-stream" | "hdfs-read" => { // deprecated connection types
                "hdfs://" +  stringVal(connection, "options.path", "/dev/null")
              }
            }

            var options: scala.collection.immutable.Map[String, String] = scala.collection.immutable.Map[String, String]()

            val filesFormatConn = stringVal(connection, "options.filesFormat", "")
            val filesFormatFlow = stringVal(flow, "options.filesFormat", "")
            val filesFormat = (
              if (filesFormatFlow != "") filesFormatFlow
              else if (filesFormatConn != "") filesFormatConn
              else "TEXT"
            )

            val format = filesFormat match {
              case "CSV" => {

                val headerConn = stringVal(connection, "options.header", "")
                val headerFlow = stringVal(flow, "options.header", "")
                options = options + ("header" -> (
                  if (headerFlow != "") headerFlow
                  else if (headerConn != "") headerConn
                  else "false")
                )

                val filesFormatCsvSeperatorConn = stringVal(connection, "options.filesFormatCsvSeperator", "")
                val filesFormatCsvSeperatorFlow = stringVal(flow, "options.filesFormatCsvSeperator", "")
                options = options + ("delimiter" -> (
                  if (filesFormatCsvSeperatorFlow != "") filesFormatCsvSeperatorFlow
                  else if (filesFormatCsvSeperatorConn != "") filesFormatCsvSeperatorConn
                  else ",")
                )

                val inferSchemaConn = stringVal(connection, "options.inferSchema", "")
                val inferSchemaFlow = stringVal(flow, "options.inferSchema", "")
                options = options + ("inferSchema" -> (
                  if (inferSchemaFlow != "") inferSchemaFlow
                  else if (inferSchemaConn != "") inferSchemaConn
                  else "true")
                )

                "csv"
              }
              case "JSON" => {
                val inferSchemaConn = stringVal(connection, "options.inferSchema", "")
                val inferSchemaFlow = stringVal(flow, "options.inferSchema", "")
                options = options + ("inferSchema" -> (
                  if (inferSchemaFlow != "") inferSchemaFlow
                  else if (inferSchemaConn != "") inferSchemaConn
                  else "true")
                )
                "json"
              }
              case "AVRO" => {
                val inferSchemaConn = stringVal(connection, "options.inferSchema", "")
                val inferSchemaFlow = stringVal(flow, "options.inferSchema", "")
                options = options + ("inferSchema" -> (
                  if (inferSchemaFlow != "") inferSchemaFlow
                  else if (inferSchemaConn != "") inferSchemaConn
                  else "true")
                )
                val ignoreExtensionConn = stringVal(connection, "options.ignoreExtension", "")
                val ignoreExtensionFlow = stringVal(flow, "options.ignoreExtension", "")
                options = options + ("ignoreExtension" -> (
                  if (ignoreExtensionFlow != "") ignoreExtensionFlow
                  else if (ignoreExtensionConn != "") ignoreExtensionConn
                  else "false") // Spark 2.4 - set ignoreExtension to false, to only read files with .avro extension
                )
                "com.databricks.spark.avro"
              }
              case "PARQUET" => {
                "parquet"
              }
              case "ORC" => {
                "orc"
              }
              case "TEXT" => {
                "text"
              }
              case "JDBC" => {
                "jdbc"
              }
              case _ => {
                filesFormat
              }
            }
            val tpat = new SimpleFile(spark, foName, filePath, format, options)

            val srcCols = vectorVal(flow, "options.projection.cols[*].from").toList.map(x => ("" + x))
            val ppat = if (srcCols.length > 0) {
              val cols = vectorVal(flow, "options.projection.cols[*].name").toList.map(x => ("" + x))
              new ProjectOp(tpat, srcCols, cols)
            } else {
              tpat
            }
            val expr = stringVal(flow, "options.expr", stringVal(flow, "options.filter.expr", "true"))
            if (expr == "true")
              Some(ppat)
            else
              Some(new FilterOp(ppat, expr))
          }
        }

      }

      // collapse a list of columns into a single struct column or array of structs
      case "pushStruct" => {
        val colName = stringVal(flow, "options.colName", stringVal(flow, "options.col", ""))
        val cols = vectorVal(flow, "options.cols").toList.map(x => ("" + x))
        val asArray = ( stringVal(flow, "options.asArray", "") == "true" )
        Some(new PushDownOp(sources(0).get, colName, cols, asArray))
      }

      case "popStruct" => {
        val col = stringVal(flow, "options.col", "")
        Some(new PopStruct(sources(0).get, col))
      }

      case "freshestRows" => {
        val keyCols = vectorVal(flow, "options.keyCols").toList.map(x => ("" + x))
        val timestamp = stringVal(flow, "options.timestamp", "")
        Some(new FreshestRowsOp(sources(0).get, keyCols, timestamp))
      }

      case "project" => {
        val srcCols = vectorVal(flow, "options.projection.cols[*].from").toList.map(x => ("" + x))
        val cols = vectorVal(flow, "options.projection.cols[*].name").toList.map(x => ("" + x))
        Some(new ProjectOp(sources(0).get, srcCols, cols))
      }

      case "join" => {
        // val sources = vectorVal(flow, "signals.sources[*].signalId").toList.map(x => {
        //   x match {
        //     case name: String => getUpstreamOp(name, spark)
        //   }
        // })
        val cols = vectorVal(flow, "options.cols").toList.map(x => ("" + x))
        val left = sources(0)
        val right = sources(1)
        val joinType = stringVal(flow, "options.joinType", "inner")
        Some(new JoinOp(left.get, right.get, cols, joinType))
      }

      case "union" => {
        // val sources = vectorVal(flow, "signals.sources[*].signalId").toList.map(x => {
        //   x match {
        //     case name: String => getUpstreamOp(name, spark)
        //   }
        // })
        val left = sources(0)
        val right = sources(1)
        Some(new UnionOp(left.get, right.get))
      }

      case "filter" => {
        val expr = stringVal(flow, "options.expr", stringVal(flow, "options.filter.expr", "true"))
        Some(new FilterOp(sources(0).get, expr))
      }

      case "dropColumns" => {
        val cols = vectorVal(flow, "options.cols").toList.map(x => ("" + x))
        Some(new ColsFilterOp(sources(0).get, cols))
      }

      case "withColumn" => {
        val cols = vectorVal(flow, "options.cols").
          toList.map( x => { Tuple2[String,String]( stringVal(x,  "name", "-"), stringVal(x, "expr", "-"))  })
        if (cols.length > 0) {
          Some(new WithColumnsOp(sources(0).get, cols))
        } else {
          val col = stringVal(flow, "options.col", "")
          val expr = stringVal(flow, "options.expr", "true")
          Some(new WithColumnOp(sources(0).get, col, expr))
        }
      }

      case "withColumns" => {
        val cols = vectorVal(flow, "options.cols").
          toList.map( x => { Tuple2[String,String]( stringVal(x,  "name", "-"), stringVal(x, "expr", "-"))  })
        if (cols.length > 0) {
          Some(new WithColumnsOp(sources(0).get, cols))
        } else {
          val col = stringVal(flow, "options.col", "")
          val expr = stringVal(flow, "options.expr", "true")
          Some(new WithColumnOp(sources(0).get, col, expr))
        }
      }

      case "withColumnRenamed" => {
        val from = stringVal(flow, "options.from", "")
        val to = stringVal(flow, "options.to", "")
        Some(new WithColumnRenamedOp(sources(0).get, from, to))
      }

      case "ruleScript" => {
        val lang = stringVal(flow, "options.language", "python")
        val methodNames = vectorVal(flow, "options.methodNames").toList.map(x => ("" + x))
        val paths = vectorVal(flow, "options.paths").toList.map(x => ("" + x))
        val locationType = stringVal(flow, "options.locationType", "hdfs")
        val base64Script = stringVal(flow, "options.base64Script", "")
        val dependenciesFolders = vectorVal(flow, "options.dependenciesFolders").toList.map(x => ("" + x))
        Some(new RuleScript(spark, sources(0).get, lang, methodNames, locationType, paths, base64Script, dependenciesFolders))
      }

      case "sink" | "writer" => makeWriter


      case "jsonSink" => {
        val from = stringVal(flow, "options.prefetch.from", "")
        val to = stringVal(flow, "options.prefetch.to", "")
        val save = (stringVal(flow, "options.prefetch.save", "false") == "true")
        val catchupIncrement = stringVal(flow, "options.catchupIncrement", "1 day")
        val refreshInterval = TimeUtils.timeStringAsMs(stringVal(flow, "refreshInterval", "15 minutes"))
        val path = stringVal(flow, "path", "/dev/null")
        Some(new JsonSink(spark, foName, sources(0).get, from, to, save, catchupIncrement, refreshInterval, path))
      }

      case "sort" => {
        val expr = stringVal(flow, "options.expr", "true")
        Some(new SortOp(sources(0).get, expr))
      }

      case "select" => {
        val cols = vectorVal(flow, "options.cols").toList.map(x => ("" + x))
        Some(new SelectOp(sources(0).get, cols))
      }

      case "limit" => {
        val rows = stringVal(flow, "options.rows", "1")
        Some(new LimitOp(sources(0).get, rows.toInt))
      }
      case _ => {
        // println(s"no match for $foType")
        None
      }
    }
    fo
  }

  // get the operator which is on the upstream side of a signal
  def getUpstreamOp(name: String, spark: org.apache.spark.sql.SparkSession, path: List[String] = List[String]()): Option[Table] = {
    // println(s"getFlowOp for $name")
    val q = "$.operations[*][?(@.signals.sinks[?(@.signalId == '" + name + "')])]"
    val so = subObjects(q, ast)
    // println(s"flow $q is $so")
    if (so.length > 0) {
      val fo = flowOpFromJobj(so(0), name, spark, path)
      fo  match {
        case Some(table) => {
          usfoDict.put(name, table)
          fo
        }
        case None => {
          log.error(s"getUpstreamOp with signalId $name could not create flow object")
          None
        }
      }
    } else {
      log.error(s"getUpstreamOp with signalId $name was not found")
      None
    }
  }


  // get the operator which is on the downstream side of a signal
  def getDownstreamOp(name: String, spark: org.apache.spark.sql.SparkSession): Option[Table] = {
    // println(s"getFlowOp for $name")
    val q = "$.operations[*][?(@.signals.sources[?(@.signalId == '" + name + "')])]"
    val so = subObjects(q, ast)
    // println(s"flow sink $q is $so")
    if (so.length > 0) {
      val fo = flowOpFromJobj(so(0), name, spark)
      fo  match {
        case Some(table) => {
          dsfoDict.put(name, table)
          fo
        }
        case None => {
          log.error(s"getDownstreamOp with signalId $name could not create flow object")
          None
        }
      }
    } else {
      log.error(s"getDownstreamOp with signalId $name was not found")
      None
    }
  }
}

object FlowSpecParse extends Serializable {

  def apply(stream: InputStream) : FlowSpecParse = {

    val bufferedSource = Source.fromInputStream(stream)

    val content = bufferedSource.mkString
    bufferedSource.close

    val mapper = new ObjectMapper

    val jobj = mapper.readValue(content, classOf[Object])
    new FlowSpecParse(jobj.asInstanceOf[Map[String, Any]])

  }

  def apply(content: String) : FlowSpecParse = {

    val mapper = new ObjectMapper

    val jobj = mapper.readValue(content, classOf[Object])

    new FlowSpecParse(jobj.asInstanceOf[Map[String, Any]])

  }
}
