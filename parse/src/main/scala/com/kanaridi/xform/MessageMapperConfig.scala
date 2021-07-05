/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.xform

import java.io.InputStream
import java.util.Map

import scala.io.Source
import scala.util.parsing.json.JSONObject

import com.fasterxml.jackson.databind.ObjectMapper

import com.kanaridi.dsql._
import com.kanaridi.common.util.TimeUtils

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.SparkSession


case class SourceMap (
  sourceName: String,
  templates: Vector[Any],
  when: String,
  idKeyExprs: Vector[Any],
  idocKeyExprs: Vector[Any],
  sortKeyExprs: Vector[Any]
)


/**  represents the mappings specification found in a mappings.json file
  */
class MessageMapperConfig(json: Map[String, Any]) extends JObjExplorer with Serializable {
  val ast: Map[String, Any] = json
  val log: Log = LogFactory.getLog(MessageMapperConfig.getClass.getName)
  val schemaVersionVal = stringVal(ast, "schemaVersion", "")
  val versionVal = stringVal(ast, "version", "0.2.0")
  val version = if (!schemaVersionVal.isEmpty) schemaVersionVal else versionVal


  def getList(expr: String, default : Vector[Any]) : Vector[Any] = {
    val m1: Vector[Any] =  children(expr, ast)
    m1
  }

  def getString(expr: String, default: String): String = {
    stringVal(ast, expr, default)
  }

  def getTemplate(name: String): Any = {
    val q = "$.templates[*][?(@.name == '" + name + "')]"
    val so = subObjects(q, ast)
    log.debug(s"template $q is $so")
    so(0)
  }


  def flowOpFromJobj(jobj: Any, spark: org.apache.spark.sql.SparkSession): Option[Table] = {

    val flow = jobj

    def flowSource(): Table = {
      println(s"flowSource: flow is $flow")

      val sources = subObjects("$.source", flow).toList.map(x => {
        println(s"flowSource: source is $x")
        x match {
          case name: String => getFlowOp(name, spark)
          case obj: Any => flowOpFromJobj(obj, spark)
        }
      })
      sources(0).get
    }


    val foName = stringVal(flow, "name", "anonymous")
    val foType = stringVal(flow, "type", "t?")

    // log.info(s"flow $foName has type $foType from $flow")
    // println(s"flow $foName has type $foType from $flow")

    val fo = foType match {
      case "partitionedAvroSource" => {
        val srcCols = vectorVal(flow, "srcCols").toList.map(x => ("" + x))
        val cols = vectorVal(flow, "cols").toList.map(x => ("" + x))
        val keyCols = vectorVal(flow, "keyCols").toList.map(x => ("" + x))
        val filePath = stringVal(flow, "filePath", "---")
        val srcTimestamp = stringVal(flow, "srcTimestamp", "")
        val timestamp = stringVal(flow, "timestamp", "")
        val filter = stringVal(flow, "filter", "")
        val fromMonth = stringVal(flow, "frommonth", "")
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
      case "simpleJsonResource" => {
        val filePath = stringVal(flow, "filePath", "---")
        val tpat = new SimpleJsonResource(spark, foName, filePath)
        Some(tpat)
      }
      case "simpleJsonFile" => {
        val filePath = stringVal(flow, "filePath", "---")
        val tpat = new SimpleJsonFile(spark, foName, filePath)
        Some(tpat)
      }
      case "simpleCsvFile" => {
        val filePath = stringVal(flow, "filePath", "---")
        val tpat = new SimpleCsvFile(spark, foName, filePath)
        Some(tpat)
      }

      case "popStruct" => {
        val col = stringVal(flow, "col", "")
        Some(new PopStruct(flowSource, col))
      }
      case "project" => {
        val srcCols = vectorVal(flow, "srcCols").toList.map(x => ("" + x))
        val cols = vectorVal(flow, "srcCols").toList.map(x => ("" + x))
        Some(new ProjectOp(flowSource, srcCols, cols))
      }
      case "join" => {
        val sources = vectorVal(flow, "source").toList.map(x => {
          x match {
            case name: String => getFlowOp(name, spark)
            case obj: Any => flowOpFromJobj(obj, spark)
          }
        })
        val cols = vectorVal(flow, "cols").toList.map(x => ("" + x))
        val left = sources(0)
        val right = sources(1)
        val joinType = stringVal(flow, "joinType", "inner")
        Some(new JoinOp(left.get, right.get, cols, joinType))
      }
      case "filter" => {
        val expr = stringVal(flow, "expr", "true")
        Some(new FilterOp(flowSource, expr))
      }
      case "withColumn" => {
        val col = stringVal(flow, "col", "")
        val expr = stringVal(flow, "expr", "true")
        Some(new WithColumnOp(flowSource, col, expr))
      }
      case "withColumnRenamed" => {
        val from = stringVal(flow, "from", "")
        val to = stringVal(flow, "to", "")
        Some(new WithColumnRenamedOp(flowSource, from, to))
      }
      case "ruleScript" => {
        val lang = stringVal(flow, "language", "python")
        val methodNames = vectorVal(flow, "methodNames").toList.map(x => ("" + x))
        val paths = vectorVal(flow, "paths").toList.map(x => ("" + x))
        val locationType = stringVal(flow, "locationType", "hdfs")
        val base64Script = stringVal(flow, "base64Script", "")
        val dependenciesFolders = vectorVal(flow, "dependenciesFolders").toList.map(x => ("" + x))
        Some(new RuleScript(spark, flowSource, lang, methodNames, locationType, paths, base64Script, dependenciesFolders))
      }
      case "eventSink" => {
        val from = stringVal(flow, "prefetch.from", "")
        val to = stringVal(flow, "prefetch.to", "")
        val save = (stringVal(flow, "prefetch.save", "false") == "true") 
        val catchupIncrement = stringVal(flow, "catchupIncrement", "1 day")
        val refreshInterval = TimeUtils.timeStringAsMs(stringVal(flow, "refreshInterval", "15 minutes"))
        val topic = stringVal(flow, "topic", "order_details")
        Some(new EventSink(spark, foName, flowSource, from, to, save, catchupIncrement, refreshInterval, topic))
      }
      case "jsonSink" => {
        val from = stringVal(flow, "prefetch.from", "")
        val to = stringVal(flow, "prefetch.to", "")
        val save = (stringVal(flow, "prefetch.save", "false") == "true") 
        val catchupIncrement = stringVal(flow, "catchupIncrement", "1 day")
        val recheckPause = TimeUtils.timeStringAsMs(stringVal(flow, "recheckPause", "15 minutes"))
        val path = stringVal(flow, "path", "/dev/null")
        Some(new JsonSink(spark, foName, flowSource, from, to, save, catchupIncrement, recheckPause, path))
      }
      case "sort" => {
        val expr = stringVal(flow, "expr", "true")
        Some(new SortOp(flowSource, expr))
      }
      case "select" => {
        val cols = vectorVal(flow, "cols").toList.map(x => ("" + x))
        Some(new SelectOp(flowSource, cols))
      }
      case "limit" => {
        val rows = stringVal(flow, "rows", "1")
        Some(new LimitOp(flowSource, rows.toInt))
      }
      case _ => {
        println(s"no match for $foType")
        None
      }
    }
    fo
  }

  def getFlowOp(name: String, spark: org.apache.spark.sql.SparkSession): Option[Table] = {
    // println(s"getFlowOp for $name")
    val q = "$.flows[*][?(@.name == '" + name + "')]"
    val so = subObjects(q, ast)
    log.info(s"flow $q is $so")
    val flow = so(0)
    flowOpFromJobj(flow, spark)
  }

  def getTopicTemplates(topic: String): Vector[Any] = {

    val expr =  if (version < "0.3" ) {
      "$.kafka-topics[*][?(@.topic == '" + topic + "')].templates"
    } else {
      "$.topics[*][?(@.topic == '" + topic + "')].templates"
    }
    val templateNames = getList(expr, Vector[Any]())
    log.debug(s"templates for $topic via $expr are $templateNames")
    val tts = templateNames.map( n => {
      getTemplate("" + n)
    })
    log.debug(s"got templates $tts")
    tts
  }

  /** parse mapping file and fetch source maps */
  def getSourceMaps(): Vector[SourceMap] = {

    val expr = "$.sourceMap[*]"

    // log.warn(s"getSourceMaps via expr $expr")
    val candidateTopics = getList(expr, Vector[Any]())

    // log.warn(s"getSourceMaps with topics:  $candidateTopics")
    // get a tuple of when expression and template
    val sourceMaps: Vector[SourceMap] = candidateTopics.map(oneMap => {
      // log.warn(s"getSourceMaps evaluate topics:  $oneMap")
      val sourceName = stringVal(oneMap, "sourceName", "")
      val whenExpr = stringVal(oneMap, "when", "")

      val idKeyExprs = vectorVal(oneMap, "idkey")
      val idocKeyExprs = vectorVal(oneMap, "idockey")
      val sortKeyExprs = vectorVal(oneMap, "sortkey")

      val templateNames = vectorVal(oneMap, "templates")
      // log.warn(s"getSourceMaps found templateNames:  $templateNames")
      val templates = templateNames.map( templ => {
        val templateName = templ.asInstanceOf[String]
        val template = getTemplate("" + templateName)
        template
      })

      log.debug(s"""got source maps: sourceName: $sourceName,
          |when: $whenExpr,
          |idkeyExprs: $idKeyExprs, idocKeyExprs: $idocKeyExprs,
          |sortKeyExprs: $sortKeyExprs"""
          .stripMargin.replaceAll("\n", " "))

      val sourceMap = new SourceMap(
        sourceName,
        templates,
        whenExpr,
        idKeyExprs,
        idocKeyExprs,
        sortKeyExprs)

      sourceMap

    })

    sourceMaps
  }


  /** return an identifier associated with a processed message
    */
  def getTopicIDString(topic: String, jobj: Any): String = {

    val exprs = if (version < "0.3" ) {
      "$.kafka-topics[*][?(@.topic == '" + topic + "')].idkey"
    } else {
      "$.topics[*][?(@.topic == '" + topic + "')].idkey"
    }

    val templateNames = getList(exprs, Vector[Any]()).asInstanceOf[Vector[String]]
    val tts = keyString(templateNames, jobj)

    tts
  }

  def getTopicSortString(topic: String, jobj: Any): String = {

    val exprs = if (version < "0.3" ) {
      "$.kafka-topics[*][?(@.topic == '" + topic + "')].sortkey"
    } else {
      "$.topics[*][?(@.topic == '" + topic + "')].sortkey"
    }

    val templateNames = getList(exprs, Vector[Any]()).asInstanceOf[Vector[String]]
    val tts = keyString(templateNames, jobj)

    tts
  }

  def getTopicIdocString(topic: String, jobj: Any): String = {

    val exprs =  if (version < "0.3" ) {
      "$.kafka-topics[*][?(@.topic == '" + topic + "')].idockey"
    } else {
      "$.topics[*][?(@.topic == '" + topic + "')].idockey"
    }

    val templateNames = getList(exprs, Vector[Any]()).asInstanceOf[Vector[String]]
    val tts = keyString(templateNames, jobj)

    tts
  }


  /** return an hbase table schema (json text) derived from a templates's row defintion
    *  per SHC's requirements
    */
  def hbaseCatalog(template: String) : String = {
    import scala.collection.immutable.Map

    val templ = getTemplate(template)
    if (version < "0.3" ) {
      val namespace = stringVal(templ, "rows[0].namespace", "x")
      val tablename = stringVal(templ, "rows[0].name", "x")
      val tdmap = Map("namespace" -> namespace, "name" -> tablename, "tableCoder" -> "PrimitiveType")
      val jtdmap = JSONObject(tdmap)
      val colFamily = stringVal(templ, "rows[0].col_family", "x")
      val vv = vectorVal(templ, "rows[0].cols[*][0]")
      val vv2 = vv.map( _.asInstanceOf[String])
      val v2 = vv2.map((x) => {  x -> JSONObject(Map[String, String]( "cf" -> colFamily, "col" -> x, "type" -> "string")) }).toMap
      val v3 = Map[String, JSONObject]( "key" -> JSONObject(Map[String, String](("cf" -> "rowkey"), ("col" -> "key"), ("type", "string"))))
      val v4 = v2 ++ v3
      val jcols = JSONObject(v4)
      val catalog = JSONObject(Map("table" -> jtdmap, "rowkey" -> "key", "columns" -> jcols))
      catalog.toString()

    } else {

      val namespace = stringVal(templ, "row.namespace", "x")
      val tablename = stringVal(templ, "row.name", "x")
      val tdmap = Map("namespace" -> namespace, "name" -> tablename, "tableCoder" -> "PrimitiveType")
      val jtdmap = JSONObject(tdmap)
      val colFamily = stringVal(templ, "row.col_family", "x")
      val vv = vectorVal(templ, "row.cols[*].name")
      val vv2 = vv.map( _.asInstanceOf[String])
      val v2 = vv2.map((x) => {  x -> JSONObject(Map[String, String]( "cf" -> colFamily, "col" -> x, "type" -> "string")) }).toMap
      val v3 = Map[String, JSONObject]( "key" -> JSONObject(Map[String, String](("cf" -> "rowkey"), ("col" -> "key"), ("type", "string"))))
      val v4 = v2 ++ v3
      val jcols = JSONObject(v4)
      val catalog = JSONObject(Map("table" -> jtdmap, "rowkey" -> "key", "columns" -> jcols))
      catalog.toString()

    }
  }
}

object MessageMapperConfig extends Serializable {

  def apply(stream: InputStream) : MessageMapperConfig = {

    val bufferedSource = Source.fromInputStream(stream)

    val content = bufferedSource.mkString
    bufferedSource.close

    val mapper = new ObjectMapper

    val jobj = mapper.readValue(content, classOf[Object])
    new MessageMapperConfig(jobj.asInstanceOf[Map[String, Any]])

  }

  def apply(content: String) : MessageMapperConfig = {

    val mapper = new ObjectMapper

    val jobj = mapper.readValue(content, classOf[Object])

    new MessageMapperConfig(jobj.asInstanceOf[Map[String, Any]])

  }
}
