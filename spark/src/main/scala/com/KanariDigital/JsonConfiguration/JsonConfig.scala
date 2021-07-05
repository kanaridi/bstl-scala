/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.JsonConfiguration

import java.io.{InputStream, InputStreamReader, Serializable}

import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}
import scala.collection.immutable.{Map => MutableMap}
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.commons.logging.{Log, LogFactory}


class JsonConfig() extends Serializable {

  var jsonObj = new JSONObject

  val log: Log = LogFactory.getLog("JsonConfig")

  def read(inputStream : InputStream, section : Option[String]) : JSONObject = {
      val jsonIn = new JSONParser().parse(new InputStreamReader(inputStream))
      jsonObj = jsonIn.asInstanceOf[JSONObject]
      if (section.isDefined) {
        jsonObj = jsonObj.get(section.get).asInstanceOf[JSONObject]
      }
      jsonObj
  }

  def readResource(path : String)  : JSONObject = {
    read(getClass.getResourceAsStream(path), None)
  }

  def getStringValue(field : String) : Option[String] = {
    val ret = jsonObj.get(field)
    if (ret.isInstanceOf[String]) Some(ret.asInstanceOf[String]) else None
  }

  def getLongValue(field : String) : Option[Long] = {
    val ret = jsonObj.get(field)
    if (ret.isInstanceOf[Long]) Some(ret.asInstanceOf[Long]) else None
  }

  def getBooleanValue(field : String) : Option[Boolean] = {
    val bStr = this.getStringValue(field)
    if(bStr.isDefined) Some(bStr.get.toLowerCase == "true") else None
  }

  def getJsonObject(field: String) : Option[JSONObject] = {
      val ret = jsonObj.get(field)
      if (ret.isInstanceOf[JSONObject]) Some(ret.asInstanceOf[JSONObject]) else None
  }

  /** get a map of key value pairs, and an empty map if the field wasnt available
    *
    * @param field - specified field which contains key value pairs
    * @returns [[Option[Map[String, String]]]]  containing key value pairs
    */
  def getMapValue(field: String): Option[Map[String, String]] = {
    val jsonObject = getJsonObject(field)
    jsonObjectToMap(jsonObject)
  }

  /** convert json object to a map */
  def jsonObjectToMap(jsonObject: Option[JSONObject]): Option[Map[String, String]] = {
    var mm = MutableMap[String, String]()
    jsonObject match {
      case Some(jobj) => {
        var iter = jobj.keySet().iterator()
        while (iter.hasNext()) {
          val k = iter.next()
          val v = jobj.get(k)
          if (v.isInstanceOf[String]) {
            mm += k.asInstanceOf[String] -> v.asInstanceOf[String]
          } else if (v.isInstanceOf[JSONArray]) {
            val jsonArray = v.asInstanceOf[JSONArray]
            val n: Int = jsonArray.size
            val jsonList: List[String] = (0 to (n-1)).map(i => jsonArray.get(i).asInstanceOf[String]).toList
            mm += k.asInstanceOf[String] -> jsonList.mkString(",")
          }
        }
        Some(mm)
      }
      case None => {
        Some(mm)
      }
    }
  }

  /** get a map of key value pairs, and an empty map if the field wasnt available
    *
    * @param field - specified field which contains key value pairs
    * @returns [[Option[Map[String, String]]]]  containing key value pairs
    */
  def getNestedMap(field: String): Option[Map[String, Map[String, String]]] = {
    val jsonObject = getJsonObject(field)
    var mm = MutableMap[String, Map[String, String]]()
    jsonObject match {
      case Some(jobj) => {
        var iter = jobj.keySet().iterator()
        while (iter.hasNext()) {
          val k = iter.next()
          val v = jobj.get(k)
          if (v.isInstanceOf[JSONObject]) {
            val valueMapOption = jsonObjectToMap(Option(v.asInstanceOf[JSONObject]))
            mm += k.asInstanceOf[String] -> valueMapOption.get
          }
        }
        Some(mm)
      }
      case None => {
        Some(mm)
      }
    }
  }

  def getJsonArray(field : String) : Option[JSONArray] = {
    val ret = jsonObj.get(field)
    if (ret.isInstanceOf[JSONArray]) Some(ret.asInstanceOf[JSONArray]) else None
  }
}
