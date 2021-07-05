/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler

import java.util.Map

import com.fasterxml.jackson.databind.ObjectMapper
import com.kanaridi.bstl.JsonPath
import org.apache.commons.logging.{Log, LogFactory}

class Dictionary(json: Map[String, Any]) extends Serializable {
  val ast: Map[String, Any] = json
  val log: Log = LogFactory.getLog(Dictionary.getClass.getName)

  // Returns a collection of JSON sub-objects for additional processing
  private def subObjects(query: String, jobj: Any): Vector[Any] = {
    log.debug(s"subObjects with $query")
    val qr = JsonPath.query(query, jobj)
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

  private def stringVal(jobj: Any, query: String, default: String): String = {
    val qr = JsonPath.query(query, jobj)
    qr match {
      case Left(x) => default
      case Right(x) => {
        val item0 = qr.right.map(_.toVector).right.get
        if (item0.length > 0 ) item0(0).toString else default
      }
    }
  }

  /**
    * Get an item in an array using json path format and key/value condition
    * @param name
    * @param key
    * @param value
    * @return
    */
  def getArrayItem(name: String, key: String, value: String): Any = {
    val q = "$." + name + "[*][?(@." + key + " == '" + value + "')]"
    val subObj = subObjects(q, ast)
    log.debug(s"template $q is $subObj")
    subObj(0)
  }

  /**
    * Get an array using json path format
    * @param expr
    * @return
    */
  def getArray(expr: String): Vector[Any] = {
    val q = "$." + expr
    children(q, ast)
  }

  /**
    * Get value in the config using json path format.
    * Return the value associate with the key or None if the key is not in the config
    * @param expr
    * @return
    */
  def get(expr: String): String = {
    val default = "N/A"
    val q = "$." + expr
    stringVal(ast, q, default)
  }
}

object Dictionary  extends Serializable {
  val log: Log = LogFactory.getLog(Dictionary.getClass.getName)
  var mockDataMap = scala.collection.mutable.Map[String,String]()

  def apply(fileName: String): Dictionary = {
    val configContent = mockDataMap.get(fileName) match {
      case Some(i) => {
        log.warn(s"Using Mock Data for Dictionary $fileName")
        i }
      case None => {
        val path = ConfigHelper.getConfigHelper().getDependencyStoragePath()
        val userStorage = StorageFactory.get(path)
        userStorage.read(fileName)
      }
    }
    
    val mapper = new ObjectMapper
    val jsonObject = mapper.readValue(configContent, classOf[Object])
    new Dictionary(jsonObject.asInstanceOf[Map[String, Any]])
  }

  def setMockData(mockPath: String, data: String) {
    mockDataMap.put(mockPath, data)
  }
}
