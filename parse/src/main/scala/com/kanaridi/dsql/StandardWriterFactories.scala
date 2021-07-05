/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import com.kanaridi.common.util.HdfsUtils
import com.kanaridi.xform.JObjExplorer
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.Schema._
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

/** a dictionary of common DataFrame writer factories, keyed by connectionType
  *   These factories are functions taking 3 arguments:
  *    1) the connectionType name,
  *    2) the json object spec for the FlowOp using the output connection, and
  *    3) the json object spec for the connection
  *    the factory function extracts local configuration options from the flowop and connection specs,
  *    returning a function that will write dataframe contents to the connection
  */
object StandardWriterFactories extends JObjExplorer {

  val log: Log = LogFactory.getLog(StandardWriterFactories.getClass.getName)

  /** a writer that cheerfully does nothing */
  def nullWriter(df: DataFrame): Long = {
    0
  }

  /** a factory for writers that do nothing */
  def nullFactory(connectionType: String, flowOp: Any, connection: Any) : (DataFrame) => Long = {
    nullWriter
  }

  /** builds writers for hdfs, abfs, etc. */
  def filesWriterFactory(connectionType: String, flowOp: Any, connection: Any) : (DataFrame) => Long = {

    val filePath = connectionType match {
      case "blob-storage" => {

        val pathConn = stringVal(connection, "options.path", "")
        val pathFlow = stringVal(flowOp, "options.path", "")
        val path = (if (pathConn != "" && pathFlow != "") HdfsUtils.resolvePaths(pathConn, pathFlow).toString
        else if (pathConn != "" && pathFlow == "") HdfsUtils.resolvePaths(pathConn).toString
        else if (pathConn == "" && pathFlow != "") HdfsUtils.resolvePaths(pathFlow).toString
        else "/dev/null")

        "abfs://" + stringVal(connection, "connection.connectionStrings.container", "none") + "@" +
        stringVal(connection, "connection.connectionStrings.storageAccount", "nobody.dfs.windows.net") +
        path
      }
      case "blob-storage-stream" => { // deprecated name
        "abfs://" + stringVal(connection, "connection.connectionStrings.container", "none") + "@" +
        stringVal(connection, "connection.connectionStrings.storageAccount", "nobody.dfs.windows.net") +
        stringVal(connection, "options.path", "/dev/null")
      }
      case "blob-storage-write" => {
        "abfs://" + stringVal(connection, "connection.connectionStrings.container", "none") + "@" +
        stringVal(connection, "connection.connectionStrings.storageAccount", "nobody.dfs.windows.net") +
        stringVal(connection, "options.path", "/dev/null")
      }
      case "hdfs-write" => {
        "hdfs://" +  stringVal(connection, "options.path", "/dev/null")
      }
      case _ => {
        "hdfs://" +  stringVal(connection, "options.path", "/dev/null")
      }
    }

    var options = scala.collection.immutable.Map[String, String]()

    val compressionConn = stringVal(connection, "options.compression", "")
    val compressionFlow = stringVal(flowOp, "options.compression", "")
    options = options + ("codec" -> (
      if (compressionFlow != "") compressionFlow
      else if (compressionConn != "") compressionConn
      else "none")
    )

    val saveModeConn = stringVal(connection, "options.saveMode", "")
    val saveModeFlow = stringVal(flowOp, "options.saveMode", "")
    val svm = (
      if (saveModeFlow != "") saveModeFlow
      else if (saveModeConn != "") saveModeConn
      else "overwrite"
    )

    val saveMode = svm match {
      case "overwrite" => SaveMode.Overwrite
      case "ignore" => SaveMode.Ignore
      case "errExists" => SaveMode.ErrorIfExists
      case _ => SaveMode.Append
    }

    val partitionColsConn = vectorVal(connection, "options.partitionCols").toList.map(x => ("" + x))
    val partitionColsFlow = vectorVal(flowOp, "options.partitionCols").toList.map(x => ("" + x))
    val partitionCols = (if (partitionColsFlow.size > 0) partitionColsFlow
    else if (partitionColsConn.size > 0) partitionColsConn
    else List[String]())
    
    val filesFormatConn = stringVal(connection, "options.filesFormat", "")
    val filesFormatFlow = stringVal(flowOp, "options.filesFormat", "")
    val filesFormat = (
      if (filesFormatFlow != "") filesFormatFlow
      else if (filesFormatConn != "") filesFormatConn
      else "TEXT"
    )
    val format = filesFormat match {
      case "CSV" => {

        val headerConn = stringVal(connection, "options.header", "")
        val headerFlow = stringVal(flowOp, "options.header", "")
        options = options + ("header" -> (
          if (headerFlow != "") headerFlow
          else if (headerConn != "") headerConn
          else "true")
        )

        val filesFormatCsvSeperatorConn = stringVal(connection, "options.filesFormatCsvSeperator", "")
        val filesFormatCsvSeperatorFlow = stringVal(flowOp, "options.filesFormatCsvSeperator", "")
        options = options + ("delimiter" -> (
          if (filesFormatCsvSeperatorFlow != "") filesFormatCsvSeperatorFlow
          else if (filesFormatCsvSeperatorConn != "") filesFormatCsvSeperatorConn
          else ",")
        )

        "csv"
      }
      case "JSON" => {
        "json"
      }
      case "AVRO" => {
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

    val writeSchemaConn = stringVal(connection, "options.writeSchema", "")
    val writeSchemaFlow = stringVal(flowOp, "options.writeSchema", "")
    val writeSchema = (
      if (writeSchemaFlow != "") (writeSchemaFlow == "true")
      else if (writeSchemaConn != "") (writeSchemaConn == "true")
      else false
    )

    def writeAvroSchema(df: DataFrame, path: String): Unit = {

      // write updated avro schema
      var schemaPath = ""

      Try {

        val dfSchema = df.schema
        val hdfsPath = new Path(path)
        schemaPath = HdfsUtils.resolvePaths(hdfsPath, hdfsPath.getName() + ".avsc").toString
        val schemaNamespace = "com.kanaridi.landed.avro"
        val schemaName = "landed"
        val recordBuilder = SchemaBuilder.record(schemaName).namespace(schemaNamespace)
        val schema: Schema = AvroSchemaConverters.convertStructToAvro(df.schema, recordBuilder, schemaNamespace)

        // set default value if the type is string
        val fields = schema.getFields().asScala
        val newFields = fields.map(field => {
          val defValue: JsonNode = if (field.schema != null &&
            ( field.schema.getType().equals(Type.STRING) ||
              ( field.schema.getType.equals(Type.UNION)
                && field.schema.getTypes().asScala.toList.map(x => x.getType()).contains(Type.STRING)))) {
            val objectMapper = new ObjectMapper()
            objectMapper.readTree(""""""""")
          } else {
            // unchanged
            field.defaultValue()
          }
          new Field(field.name(), field.schema(), field.doc(), defValue)
        })

        val newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(),
          schema.isError())
        newSchema.setFields(newFields.asJava)
        val jsonSchema = newSchema.toString(true) // pretty = true

        HdfsUtils.saveFileContents(schemaPath, List(jsonSchema))
      } match {
        case Success(x) => {
          log.warn(s"successfully wrote avro schema to $schemaPath")
        }
        case Failure(ex) => {
          log.error(s"failed writing avro schema")
          ex.printStackTrace()
        }
      }
    }

    def blobWriter (df: DataFrame) : Long = {

      if (format == "com.databricks.spark.avro") {
        val compressOpt = options.getOrElse("codec", "snappy")
        val compression = if (compressOpt == "none") "uncompressed" else compressOpt
        df.sparkSession.conf.set("spark.sql.avro.compression.codec", compression)
      }

      log.warn(s"blobWriter: write dataframe start - filePath: $filePath, format: $format"
        + s"format: $format, options: $options, partitionCols: $partitionCols")
      df.show()

      val writer = if (partitionCols.length > 0)
        df.coalesce(1).write.mode(saveMode).format(format).options(options).partitionBy(partitionCols:_*)
      else
        df.write.mode(saveMode).format(format).options(options)

      writer.save(filePath)
      log.warn(s"blobWriter: write dataframe done - filePath: $filePath, format: $format"
        + s"options: $options, partitionCols: $partitionCols")

      log.warn(s"blobWriter: write schema start - filePath: $filePath, format: $format")
      if (format == "com.databricks.spark.avro" && writeSchema) {
        writeAvroSchema(df, filePath)
        log.warn(s"blobWriter: write schema done - filePath: $filePath")
      } else {
         log.warn(s"blobWriter: write schema skipped - filePath: $filePath, format: $format, writeSchema: $writeSchema")
      }

      0
    }
    blobWriter
  }

  /** writes a key + json text blob to a two column table */
  def jdbcWriterFactory(connectionType: String, flowOp: Any, connection: Any): (DataFrame) => Long = {
    import java.util.Properties
    import org.apache.spark.sql.functions.{lit, col, to_json, concat, when, struct}
    import org.apache.spark.sql.types._

    //If the data type is not in this list, it will be converted to JSON. The notable types NOT here
    // are array and struct which most jdbc implementations choke on.
    val nonJsonConvertedTypes = Seq(DataTypes.ByteType, DataTypes.BooleanType, DataTypes.CalendarIntervalType, DataTypes.DateType,
      DataTypes.DoubleType, DataTypes.FloatType, DataTypes.IntegerType, DataTypes.LongType, DataTypes.NullType,
      DataTypes.ShortType, DataTypes.StringType, DataTypes.TimestampType)

    val sqlUser = stringVal(connection, "connection.security.userNameAndPassword.userName", "nobody")
    val sqlPass = stringVal(connection, "connection.security.userNameAndPassword.password", "nobody")
    val sqlUrl = stringVal(connection, "connection.connectionStrings.url", "")
    val driver = stringVal(connection, "connection.connectionStrings.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    val sqlTable = stringVal(flowOp, "options.sqlTable", "")
    val keyCols = vectorVal(flowOp, "options.keyCols").toList.map(x => ("" + x))
    val useJson = (stringVal(flowOp, "options.jsonSchema", "false") == "true")
    val name = stringVal(flowOp, "signals.sources[0].signalId", "anonymous")

    def writeJdbc(df: DataFrame): Long = {

      //add a column called df_key which is a concat of all primary keys
      var dfWithKey = df.distinct.withColumn("df_key", lit(""))
      keyCols.map(colName => {
        dfWithKey = dfWithKey.withColumn("df_key", concat(
          when(col("df_key").isNotNull, col("df_key")).otherwise(lit("")),
          when(col("df_key") === "", lit("")).otherwise(lit("_")), //underscore separator
          when(col(colName).isNotNull, col(colName)).otherwise(lit(""))))
      })

      val writableDf = if (useJson) {
        (dfWithKey.withColumn("flowName", lit(name)).select(col("df_key"), to_json(struct(col("*"))).alias("content")))
      } else {
        //Regardless of Json, jdbc doesn't typically support all data types so if we hit one we need to convert it to json representation regardless
        // We may be able to write some jdbc dialects in the future, but becomes complex when you get into deeply nested structures created by end users
        val columnsToConvert = dfWithKey.schema.filter(s => !nonJsonConvertedTypes.contains(s.dataType))
        val colNames = columnsToConvert.map(c=>c.name)
        val colTypes = columnsToConvert.map(c=>c.dataType.typeName.toString())
        log.warn(s"Converting these columns: ${colNames.toString()}")
        log.warn(s"with these types: ${colTypes.toString()}")
        columnsToConvert.foldLeft(dfWithKey){(foldedDf, c) => foldedDf.withColumn(c.name, to_json(col(c.name)))}
      }

      log.warn(s"Schema to write to jdbc: ${writableDf.printSchema()}")

      val svm = stringVal(flowOp, "options.saveMode", "overwrite")
      val saveMode = svm match {
        case "overwrite" => SaveMode.Overwrite
        case "ignore" => SaveMode.Ignore
        case "errExists" => SaveMode.ErrorIfExists
        case _ => SaveMode.Append
      }

      val connProps = new Properties()
      connProps.setProperty("user", sqlUser)
      connProps.setProperty("password", sqlPass)

      writableDf.write
        .mode(saveMode)
        .format("jdbc")
        .option("driver", driver)
        .jdbc(sqlUrl, sqlTable, connProps)

       0
    }

    writeJdbc
  }

  /** writes a dataframe converted to json to eventhub */
  def eventhubWriterFactory(connectionType: String, flowOp: Any, connection: Any): (DataFrame) => Long = {

    import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
    import java.time.Duration
    import org.apache.spark.sql.functions._

    val eventhubNamespace = stringVal(connection, "connection.connectionStrings.namespace", "nobody")
    val eventhubName = stringVal(connection, "connection.connectionStrings.entityName", "nobody")
    val sasKey = stringVal(connection, "connection.connectionStrings.sharedAccessKey", "nobody")
    val sasKeyName = stringVal(connection, "connection.connectionStrings.sharedAccessKeyName", "nobody")

    val operationTimeoutConn = stringVal(connection, "options.operationTimeout", "")
    val operationTimeoutFlow = stringVal(flowOp, "options.operationTimeout", "")
    val operationTimeout =  (if (operationTimeoutFlow != "") operationTimeoutFlow
      else if (operationTimeoutConn != "") operationTimeoutConn
      else "60").toInt

    val receiverTimeoutConn = stringVal(connection, "options.receiverTimeout", "")
    val receiverTimeoutFlow = stringVal(flowOp, "options.receiverTimeout", "")
    val receiverTimeout = (if (receiverTimeoutFlow != "") receiverTimeoutFlow
      else if (receiverTimeoutConn != "") receiverTimeoutConn
      else "60").toInt
 
    def writeEventhub(df: DataFrame): Long = {

      println(s"writeEventhub: write dataframe start - eventhubNamespace: $eventhubNamespace, eventhubName: $eventhubName"
        + s"sasKeyName: $sasKeyName")
      df.show()

      val connectionString = ConnectionStringBuilder()
        .setNamespaceName(eventhubNamespace)
        .setEventHubName(eventhubName)
        .setSasKeyName(sasKeyName)
        .setSasKey(sasKey)
        .build

      val ehConf: EventHubsConf = EventHubsConf(connectionString)
        .setReceiverTimeout(Duration.ofSeconds(receiverTimeout))
        .setOperationTimeout(Duration.ofSeconds(operationTimeout))

      // rename value to body
      val wdf = df.toJSON.select(col("value").as("body"))

      //println(s"writeEventhub: going to write ${wdf.count} rows to eventhub ...")
      // wdf.show()

      wdf.select("body")
        .write
      //.format("eventhubs")
        .format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider")
        .options(ehConf.toMap)
        .save()
      0
    }

    writeEventhub
  }

  def apply(): scala.collection.mutable.HashMap[String, (String, Any, Any) => (DataFrame) => Long] = {

    val factories = new scala.collection.mutable.HashMap[String, (String, Any, Any) => (DataFrame) => Long]()

    factories.put("null", nullFactory)
    factories.put("jdbc", jdbcWriterFactory)
    factories.put("blob-storage-stream", filesWriterFactory)
    factories.put("blob-storage-write", filesWriterFactory)
    factories.put("blob-storage", filesWriterFactory)
    factories.put("hdfs-write", filesWriterFactory)
    factories.put("hdfs-stream", filesWriterFactory)
    factories.put("hdfs", filesWriterFactory)
    factories.put("event-hub-write", eventhubWriterFactory)
    factories.put("event-hub", eventhubWriterFactory)

    factories
  }

}
