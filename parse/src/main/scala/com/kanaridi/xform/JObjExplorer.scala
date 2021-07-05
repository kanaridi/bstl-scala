/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.xform
import com.kanaridi.dsql._

import com.kanaridi.bstl.JsonPath
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.Map


/**  functions for applying JSONPath queries to FasterXML.jackson's generic parsed JSON object model
  */
trait JObjExplorer {

  /** Returns a collection of JSON sub-objects for additional processing */
  def subObjects(query: String, jobj: Any): Vector[Any] = {

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
  def children(query: String, jobj: Any): Vector[Any] = {
    val subObjs = subObjects(query + "[*]", jobj)
    val s01s = if (subObjs.length > 0) subObjs else subObjects(query, jobj)
    s01s
  }

  def stringVal(jobj: Any, expr: String, default: String): String = {
    val exprs = List( "$." + expr )
    val rowkey =
      exprs.map( (k) => {
        val qr = JsonPath.query(k, List(jobj))
        qr match {
          case Left(x) => default
          case Right(x) => {
            val item0 = qr.right.map(_.toVector).right.get
            if (item0.length > 0 ) item0(0) else default
          }
        }
      }).mkString("-")
    rowkey
  }

  def vectorVal(jobj: Any, query: String): Vector[Any] = {
    val subObjs = subObjects("$." + query + "[*]", jobj)
    val s01s = if (subObjs.length > 0) subObjs else subObjects("$." + query, jobj)
    s01s
  }

  def keyString(exprs: Vector[String], jobj: Any): String = {
    val rowkey =
      exprs.map( (k) => {
        val qr = JsonPath.query(k, List(jobj))
        qr match {
          case Left(x) => None
          case Right(x) => {
            val item0 = qr.right.map(_.toVector).right.get
            if (item0.length > 0 ) item0(0)
          }
        }
      }).mkString("-")
    rowkey
  }

  /** extract values from source json message given a vector bstl expressions
    * @param exprs list of bstl/json path expressions
    * @param jobj source json message
    * @return concatanated values extracted from source json
    */
  def getExprsString(exprs: Vector[Any], jobj: Any): String = {
    val bstlExprs = exprs.asInstanceOf[Vector[String]]
    val tts = keyString(bstlExprs, jobj)
    tts
  }

}
