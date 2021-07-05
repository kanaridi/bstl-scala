/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.xform

import scala.collection.immutable.Map
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.ObjectMapper

import com.kanaridi.bstl.JsonPath

import org.apache.commons.logging.{Log, LogFactory}


class MessageMapper(config: MessageMapperConfig) extends Serializable {

  val cfg = config

  val log: Log = LogFactory.getLog(this.getClass.getName)

  case class Parsed (
    jobj: Object,
    src: String,
    idoc: String,
    sortkey: String,
    topic: String
  ) extends Ordered[Parsed] {
    def compare(that: Parsed) = this.sortkey.compare(that.sortkey)
  }

  def md5HashString(s: String): String = {
    import java.security.MessageDigest
    import java.math.BigInteger
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1,digest)
    val hashedString = bigInt.toString(16)
    hashedString
  }

  private def evalExpr(expr: String, jobjs: List[Any]): Option[String] = {

    val literal = "'([^']*)'"r
    val path = "(\\$.+)"r
    val parent = "\\.\\./(\\$.+)"r
    val grandparent = "\\.\\./\\.\\./(\\$.+)"r

    expr match {
      case literal(exp) => {
        Some(exp)
      }
      case path(exp) => {
        val qr = JsonPath.query(expr, jobjs)
        qr match {
          case Left(x) => None
          case Right(x) => {
            val item0 = qr.right.map(_.toVector).right.get
            if (item0.length > 0 ) Some(item0(0).toString) else None
          }
        }
      }
      case parent(exp) => {
        val qr = JsonPath.query(expr, jobjs)
        qr match {
          case Left(x) => None
          case Right(x) => {
            val item0 = qr.right.map(_.toVector).right.get
            if (item0.length > 0 ) Some(item0(0).toString) else None
          }
        }
      }
      case grandparent(exp) => {
        val qr = JsonPath.query(expr, jobjs)
        qr match {
          case Left(x) => None
          case Right(x) => {
            val item0 = qr.right.map(_.toVector).right.get
            if (item0.length > 0 ) Some(item0(0).toString) else None
          }
        }
      }
      case _ => {
        // log.info(s"no pattern match for $expr")
        val qr = JsonPath.query(expr, jobjs)
        qr match {
          case Left(x) => None
          case Right(x) => {
            val item0 = qr.right.map(_.toVector).right.get
            if (item0.length > 0 ) Some(item0(0).toString) else None
          }
        }
        //       None
      }
    }
  }

  // finds strings in JSON object and joins into a single string
  private def keyString(exprs: List[String], jobj: List[Any]): String = {
    val rowkey =
      exprs.map( (k) => {
        evalExpr(k, jobj) match {
          case Some(x) => x
          case _ =>
        }
      }).mkString("-")
    rowkey
  }

  /** split colFamily:colName string into tuple of (colFamily, colName)
    * if colFamily is absent  return tuple of ("", colName)
    */
  private def splitColName(colNameStr: String): Tuple2[String, String] = {
    if (colNameStr.contains(":")) {
      val colFamilyNamePair = colNameStr.split(":")
      val ccf = colFamilyNamePair(0)
      val ccn = colFamilyNamePair(1)
      Tuple2(ccf, ccn)
    } else {
      val ccf = ""
      val ccn = colNameStr
      Tuple2(ccf, ccn)
    }
  }

  /** For a table, given a set of fields (colName-colValue pairs), 
    * extract columnFamily prefix from column name and then do a group by
    * column family to collect fields for a column family in a group.
    * 
    * convert each group to list of [[HBRow]]'s
    * 
    * @return a list of [[HBRow]]
    */
  private def extractAndGroupbyColFamily(table: String, rowKey: String,
    fields: Map[String, String], namespace:String, deDupHash: String,
    sortKey: String): List[HBRow] = {

    val colFamilyGroupbyMap = MutableMap[String, MutableMap[String, String]]()
      for( (colName, colValue) <- fields ) {
        // extract columnFamily and colName strings
        val (ccf, ccn) = splitColName(colName.toString)
        // group fields by columnFamily
        colFamilyGroupbyMap.get(ccf) match {
          case Some(x) => {
            val someFields = x.asInstanceOf[MutableMap[String, String]]
            someFields += (ccn -> colValue)
            colFamilyGroupbyMap += ( ccf -> someFields)
          }
          case None => {
            val newField = MutableMap[String, String]() += ( ccn -> colValue )
            colFamilyGroupbyMap += ( ccf -> newField)
          }
        }
      }
    val returnList = ListBuffer[HBRow]()
    for ( (colf, fields) <- colFamilyGroupbyMap ) {
      val immutableFields = fields.toMap
      returnList += HBRow(table, rowKey, colf, immutableFields, namespace, deDupHash, sortKey)
    }
    returnList.toList
  }

  private def getRows(rowdefs: Vector[Any], sortKey: String, jobj: List[Any]) : List[HBRow] = {

    val result = rowdefs.flatMap(templ => {

      val namespace = cfg.stringVal(templ, "namespace", "huh")
      val table = cfg.stringVal(templ, "name", "why")
      val defaultColFamily = cfg.stringVal(templ, "colFamily", "N/A")
      val cds = cfg.vectorVal(templ, "cols")

      val cols = cds.map ( x => {
        val al = x.asInstanceOf[java.util.Map[String, Any]]
        val col = al.get("name").asInstanceOf[String]
        val expr = al.get("value").asInstanceOf[String]
        val colFamily = if (al.containsKey("colFamily")) al.get("colFamily").asInstanceOf[String] else defaultColFamily
        val colName = s"$colFamily:$col"
        (colName, expr)
      }).toList

      val deDupCols = cds.filter( x => {
        val al = x.asInstanceOf[java.util.Map[String, Any]]
        val rv = al.get("isDedup")
        rv != null && rv.asInstanceOf[Boolean]
      }).map( x => {
        val al = x.asInstanceOf[java.util.Map[String, Any]]
        val col = al.get("name").asInstanceOf[String]
        val colFamily = if (al.containsKey("colFamily")) al.get("colFamily").asInstanceOf[String] else defaultColFamily
        val colName = s"$colFamily:$col"
        colName
      }).toList.asInstanceOf[List[String]]

      val fields = cols.map( (kv) => {
        (kv._1, {

          val qexp = kv._2

          evalExpr(kv._2, jobj) match {
            case Some(expVal) => if (!expVal.isEmpty) expVal else None
            case _ => None
          }
        })
      }).filter(_._2 != None).toMap.asInstanceOf[Map[String, String]]

      val rowKey = if (cfg.version < "0.4") {
        // rowkey def: bstl expr
        val keys = cfg.stringVal(templ, "rowKey", "-")
        val rk = evalExpr(keys, jobj).getOrElse("?")
        log.info(s"rowkey is $rk")
        rk
      } else {
        // rowkey def: list of column names
        val colNamesRkSpec = cfg.vectorVal(templ, "rowKey")

        // in a rowkey spec add defaultColFamily as a prefix if colFamily prefix has not been specified
        val colNames = colNamesRkSpec.map(x => if (x.toString.contains(":")) x else s"$defaultColFamily:$x")

        val rk = colNames.map( colName => {
          val colValMaybe = fields.get(colName.toString)
          colValMaybe match {
            case Some(x) => x
            case None => {
              val (ccf, ccn) = splitColName(colName.toString)
              //try to evaluate colname as bstl expression
              evalExpr(ccn.toString, jobj) match {
                case Some( expVal) => if (!expVal.isEmpty) expVal else "?"
                case _ => "?"
              }
            }
          }
        }).mkString("-")
        log.info(s"rowkey is $rk")
        rk
      }

      //      val deDupCols = cfg.vectorVal(templ, "deDupCols").toList.asInstanceOf[List[String]]
      val deDupHash = if (deDupCols.length > 0)
        md5HashString(deDupCols.map( key => fields.getOrElse(key, "?") ).mkString("-")) else ""

      // extract colFamily from colName in fields, group fields by columnFamily and create hbrows
      extractAndGroupbyColFamily(table, rowKey, fields, namespace, deDupHash, sortKey)
    })

    result.toList
  }

  private def mapRows(rowdefs: Vector[Any], sortKey: String, jobj: List[Any]) : List[HBRow] = {

    val result = rowdefs.flatMap(templ => {

      val namespace = cfg.stringVal(templ, "namespace", "huh")
      val table = cfg.stringVal(templ, "name", "why")
      val colFamily = cfg.stringVal(templ, "col_family", "N/A")
      val cds = cfg.vectorVal(templ, "cols")

      val cols = cds.map ( x => {
        val al = x.asInstanceOf[java.util.ArrayList[String]]
        val col = al.get(0).asInstanceOf[String]
        val expr = al.get(1).asInstanceOf[String]
        (col, expr)
      }).toList

      val fields = cols.map( (kv) => {
        (kv._1, {

          val qexp = kv._2

          evalExpr(kv._2, jobj) match {
            case Some(expVal) => if (!expVal.isEmpty) expVal else None
            case _ => None
          }
        })
      }).filter(_._2 != None).toMap.asInstanceOf[Map[String, String]]

      val keys = cfg.vectorVal(templ, "rowkey").toList.asInstanceOf[List[String]]
      val rowKey = keyString(keys, jobj)
      val deDupCols = cfg.vectorVal(templ, "deDupCols").toList.asInstanceOf[List[String]]
      val deDupHash = if (deDupCols.length > 0)
        md5HashString(deDupCols.map( key => fields.getOrElse(key, "?") ).mkString("-")) else ""
      List( HBRow(table, rowKey, colFamily, fields, namespace, deDupHash, sortKey) )
    })
    result.toList
  }

  private def mapRecord(templ: Any, sortKey: String, jobj : List[Any]): List[HBRow]  = {

    //    log.info(s"mapping $jobj with $templ")
    var localRows : List[HBRow] = List()

    if (cfg.version < "0.3") {
      val rowsmaps = cfg.vectorVal(templ, "rows")
      localRows = mapRows(rowsmaps, sortKey, jobj)
    } else {
      val rowdef = cfg.vectorVal(templ, "row")  // a Vector with just one element
      localRows = getRows(rowdef, sortKey, jobj)         // may return multiple HBRows
    }
    val kidSpecs = cfg.vectorVal(templ, "children")

    val kidRows = kidSpecs.flatMap( spec => {

      val templateName = cfg.stringVal(spec, "template", "none")
      val template = cfg.getTemplate(templateName)
      val select = cfg.stringVal(spec, "select", "none")

      val kids = children(select, jobj(0))

      kids.flatMap( subobj => {
        mapRecord(template, sortKey, subobj :: jobj)
      }).toList
    })

    localRows ++ kidRows
  }

  //
  // we're given the text of 0 or more JSON Objects, one per line
  def transformVersion03(topic: String, message: String) : List[TransformResult] = {

    val start = System.currentTimeMillis() // time this
    //log.info(s"transforming $message")
    val messages = message.split("\n")
      .toSeq
      .map(_.trim)
      .filter(_ != "")

    val parsedJobs = messages.map( str => {
      val jobj = (new ObjectMapper).readValue(str, classOf[Object])
      val idoc = cfg.getTopicIdocString(topic, jobj)
      val sortkey = cfg.getTopicSortString( topic, jobj )
      //log.info(s"sortkey ***: $sortkey")
      new Parsed(jobj, str, idoc, sortkey, topic)
    })

    // sort source records by sort key
    val sorted = parsedJobs.sortWith( _.compareTo(_) < 0 )

    val rowList = sorted.flatMap( parsed  => {

      val jobj = parsed.jobj
      val pkey = cfg.getTopicIDString(topic, jobj)

      // log.info(s"parsed json for $topic with m-b-s of $pkey")
      val pSortKey = parsed.sortkey

      val topicTemplates = cfg.getTopicTemplates(topic)

      val xformed = topicTemplates.flatMap(templ => {
        mapRecord(templ, pSortKey, List(jobj))
      })
      xformed
    }).toList

    val idocSet = parsedJobs.map(x => x.idoc).distinct.toSet
    val rv = new TransformResult(rowList, idocSet, topic)
    val duration = System.currentTimeMillis() - start
    log.debug(s"MessageMapper Transformed message from topic $topic in $duration milliseconds")
    List(rv)
  }

  // we're given the text of 0 or more JSON Objects, one per line
  def transform(topic: String, message: String): List[TransformResult] = {

    val start = System.currentTimeMillis() // time this

    // log.info(s"transforming $message")
    // log.warn(s"transforming $message with cfgversion = ${cfg.version}")
    if  (cfg.version < "0.4") {
      transformVersion03(topic, message)
    } else {
      val messages = message.split("\n")
        .toSeq
        .map(_.trim)
        .filter(_ != "")

      //get list of topic templates from config file for a given topic
      val sourceMaps: Vector[SourceMap] = cfg.getSourceMaps()

      // log.debug(s"sourceMaps.size(): ${sourceMaps.size}")
      // log.warn(s"sourceMaps is: ${sourceMaps}")

      //sort the incoming messages by sort expr
      val transformResultList = ListBuffer[TransformResult]()

      // cache sorted source messages
      val sortedMessagesCache = MutableMap[String, Seq[Parsed]]()

      for ( sourceMap <- sourceMaps) {
        // log.warn(s"using sourceMap: $sourceMap")
        // get the sourceName
        val sourceName = sourceMap.sourceName

        // get the sort key and idoc exprs
        val sortKeyExprs = sourceMap.sortKeyExprs
        val idocKeyExprs = sourceMap.idocKeyExprs

        // cache key (sort key + doc id key)
        val cacheKey = sortKeyExprs.mkString("^") + "^" + idocKeyExprs.mkString("^")

        // check in cache
        val sortedMessagesMaybe = sortedMessagesCache.get(cacheKey)

        val sorted:Seq[Parsed] = if (sortedMessagesMaybe.isDefined) {

          val sorted = sortedMessagesMaybe.get.asInstanceOf[Seq[Parsed]]
          sorted

        } else {
          // sort the messages by sort key expr
          val parsedJobs = messages.map( str => {
            val jobj = (new ObjectMapper).readValue(str, classOf[Object])
            val idocVal = cfg.getExprsString(idocKeyExprs, jobj)
            val sortkeyVal = cfg.getExprsString(sortKeyExprs, jobj)
            new Parsed(jobj, str, idocVal, sortkeyVal, topic)
          })

          // sort source records by sort key
          val sorted = parsedJobs.sortWith( _.compareTo(_) < 0 )

          // add sorted messages to cache
          sortedMessagesCache += cacheKey -> sorted

          // return messages
          sorted
        }

        // transform source messages by all source map's where when expr is true
        val rowList = sorted.flatMap( parsed  => {
          val jobj = parsed.jobj
          val applyTemplates: Vector[Any] = if ( canApplySourceMap(topic, sourceMap, jobj) ) {
            val templates: Vector[Any] = sourceMap.templates
            templates
          } else {
            Vector[Any]()
          }
          // log.info(s"parsed json for $topic with m-b-s of $pkey")
          // log.warn(s"parsed json for $topic will apply applyTemplates")
          val pSortKey = parsed.sortkey
          val xformed = applyTemplates.flatMap(templ => {
            mapRecord(templ, pSortKey, List(jobj)) //pSortKey is pass through var and added to hbrow
          })
          xformed
        }).toList

        // add TransformResult only if transformation produced hbrows
        if (rowList.size > 0) {
          // println("adding transform result to the list ...")
          val idocSet = sorted.map(x => x.idoc).distinct.toSet
          val rv = new TransformResult(rowList, idocSet, topic)
          transformResultList += rv
        }
      }
      val duration = System.currentTimeMillis() - start
      log.debug(s"MessageMapper Transformed message from topic $topic in $duration milliseconds")
      transformResultList.toList
    }
  }

  /** get map of context variables */
  private def getContextMap(keyValVars: Tuple2[String, String]*): Map[String, String] = {
    // convert Tuple2(String, String) to a map
    val context = keyValVars.toMap
    context
  }

  /** return sourceMap if evaluating {{ sourceMap }}'s when expr is true */
  private def canApplySourceMap(topic: String, sourceMap: SourceMap, jobj: Any): Boolean = {

    val sourceName = sourceMap.sourceName
    val templates = sourceMap.templates
    val whenExpr = sourceMap.when.toString

    // log.debug(s"putting topic: $topic in the context")

    // get context variable map
    val context: Map[String, String] = getContextMap(

      // tuple of key value pairs that should be added to
      // the context
      Tuple2("topic", topic),
      Tuple2("sourceName", sourceName)

    )

    // evaluate when expression
    val qr = JsonPath.eval(whenExpr, context, List(jobj))
    val result = qr match {
      case Left(x) => None
      case Right(x) => {
        val item0 = qr.right.map(_.toVector).right.get
        if (item0.length > 0 ) Some(item0(0).toString) else None
      }
    }

    result match {
      case Some(result) => {
        Try { result.toBoolean } match  {
          case Success(x) => {
            // log.debug(s"template {$sourceName}: {$whenExpr} returning {$result} ")
            x
          }
          case Failure(ex) => {
            false
          }
        }
      }
      case None => {
        false
      }}
  }

  // Returns a collection of JSON sub-objects for additional processing
  private def subObjects(query: String, jobj: Any): Vector[Any] = {
    val qr = JsonPath.query(query, List(jobj))
    qr match {
      case Left(x) => Vector[Any]()
      case Right(x) => {
        val item0 = qr.right.map(_.toVector).right.get
        item0
      }
    }
  }

  // Tries to find an array at the node addressed by query,
  // failing that, returns the single object in a Vector of length 1
  private def children(query: String, jobj: Any): Vector[Any] = {
    val subObjs = subObjects(query + "[*]", jobj)
    val s01s = if (subObjs.length > 0) subObjs else subObjects(query, jobj)
    s01s
  }

}
