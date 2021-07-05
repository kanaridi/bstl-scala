/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.rest

import com.KanariDigital.app.AppMain.log
import com.kanaridi.common.util.HdfsUtils
import org.apache.spark.sql.SparkSession
import com.kanaridi.dsql.FlowSpecParse

import scala.util.{Failure, Success, Try}

object Service {

  import scala.io.Source
  import com.kanaridi.xform._
  import java.io.{InputStream, ByteArrayInputStream, InputStreamReader, FileInputStream}
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._

  var spark: SparkSession = null

  val DEFAULT_LIMIT = 20

  /** return the transformation of a sample bit of source data with the provided mappings
    */
  def runMap(topic: String, mapFile: String, srcData: String) : String= {

    val configStream =  new ByteArrayInputStream(mapFile.getBytes)
    val mmcfg = MessageMapperConfig(configStream)
    val xform = MessageMapperFactory(mmcfg)

    val allRows = xform.transform(topic, srcData).map(transformResult => {

      val rows = transformResult.hbaseRows

      val rl = rows.map( row => {
        val table = row.tableName
        val key = row.rowKey
        val namespace = row.namespace
        val family = row.family
        val dedup = row.deDupKey
        // println(s"row: $namespace : $table, $family, $key, deDupKey = $dedup")
        val rv = (
          ("tableName" -> table) ~
            ("rowKey" -> key) ~
            ("tableNamespace" -> namespace) ~
            ("colFamily" -> family) ~
            ("dedupHash" -> dedup) ~
            ("cols" -> row.cells)
        )

        val jsonResult = compact(render(rv))
        jsonResult

      }).mkString(",\n")

      s"""{\"rows\": [ $rl ]}"""

    }).mkString(",\n") //allRows

    s"""[$allRows]"""
  }

  /** return a JSON representation of the first [limit] rows from the given sinks.signalID
    *  in the provided flowspec file along with some useful metadata
    */
  def runFlowV2(flowSource: String, sinkName: String, limit: Int = DEFAULT_LIMIT,
    disableWrites: Boolean = false, previewMode: Boolean = false, disableEmbargo: Boolean = false, addCount: Boolean = false): String = {
    if (spark == null) {
      spark = SparkSession.builder.appName("FlowV2").getOrCreate()
      val sparkContext = spark.sparkContext
      val appLogPath = HdfsUtils.getAppDir("flowapi", "")
      if (appLogPath.isDefined) {
        val dirName = appLogPath.get.toUri.getPath
        log.warn(s"creating checkpoint directory: $dirName")
        HdfsUtils.createDirectory(dirName)
        // set checkpoint directory
        sparkContext.setCheckpointDir(dirName)
      }
    }
    val mmcfg = FlowSpecParse(flowSource)

    if (disableWrites) {
      mmcfg.disableWrites
      //set preview mode
      if (previewMode) {
        mmcfg.setPreviewMode
      }
    }
    if (disableEmbargo) {
      mmcfg.disableEmbargo
    }

    val flowOpMaybe = mmcfg.getUpstreamOp(sinkName, spark)

    if (flowOpMaybe.isEmpty) {
      val errorMessage = s""""could not instantiate a flow object""""
      val errorType = s""""SparkException""""
      val resultError = s"""{"sinkSigId": "$sinkName",
"limit": $limit,
"errors": [{"message": $errorMessage, "details": $errorMessage, "errorType": $errorType}],
"errorContentSchema": "",
"errorContent": [] }
"""
      resultError
    }

    val flowOp = flowOpMaybe.get

    flowOp.all match {
      case Some(x) => {

        val resultDf = x

        val numRows = if (addCount) {
          resultDf.cache
          resultDf.count
        } else {
          -1
        }

        if (flowOp.isInstanceOf[com.kanaridi.dsql.Sink]) {
          if (!disableWrites) {
            flowOp.asInstanceOf[com.kanaridi.dsql.Sink].send(resultDf)
          }
        }
        if (resultDf.columns.toList.contains("__error__")) {

          val errors =
            resultDf.select(
              resultDf("__error__").alias("message"),
              resultDf("__errorstack__").alias("details"),
              resultDf("__errortype__").alias("errorType")
            ).distinct.toJSON.collect.mkString(",\n")

          val dataDf = resultDf
            .drop(resultDf("__error__"))
            .drop(resultDf("__errorstack__"))
            .drop(resultDf("__errortype__"))

          // drop error and errorstack
          val schemaStr = dataDf.schema.json
          val data = dataDf.limit(limit).toJSON.take(limit).mkString(",\n")

          // FIXME: prolly ought to construct this more like runMap(), above
          val resultError = s"""{"sinkSigId": "$sinkName",
"limit": $limit,
"numRows": $numRows,
"errors": [ $errors ],
"errorContentSchema": $schemaStr,
"errorContent": [ $data ] }
"""
          resultError
        } else {
          val schemaStr = resultDf.schema.json
          val data =  resultDf.limit(limit).toJSON.take(limit).mkString(",\n")
          // FIXME: prolly ought to construct this more like runMap(), above
          val result = s"""{"sinkSigId": "$sinkName",
"limit": $limit,
"numRows": $numRows,
"schema": $schemaStr,
"content": [ $data ] }
"""
          resultDf.unpersist
          result
        }
      }
      case None => {

        val errorMessage = s""""flow op ${flowOp.getClass.getSimpleName} returned no data""""
        val errorType = s""""SparkException""""
        val resultError = s"""{"sinkSigId": "$sinkName",
"limit": $limit,
"errors": [{"message": $errorMessage, "details": $errorMessage, "errorType": $errorType}],
"errorContentSchema": "",
"errorContent": [] }
"""
        resultError
      }
    }
  }

  // /**  @deprecated */
  // def getManifest() = {
  //   val stream = new FileInputStream("spark.manifest.json")
  //   val manifestJson = parse(new InputStreamReader(stream))
  //   stream.close()
  //   manifestJson
  // }
}
