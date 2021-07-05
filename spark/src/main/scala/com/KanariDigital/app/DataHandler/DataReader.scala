/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler

import java.time.Instant

import com.KanariDigital.Orderbook.BpRulesRunner
import com.KanariDigital.app.AppContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataReaderConfig(val configPath: String,
                       val validationMapper: Dictionary) extends Serializable

final case class MissingMappingException(private val message: String = "") extends Exception(message)
final case class InvalidFormatException(private val message: String = "") extends Exception(message)


class DataReader(ss: SparkSession, dataReaderConfig: DataReaderConfig) {
  var highLevelObjectDF: DataFrame = null
  val log: Log = LogFactory.getLog(DataReader.getClass.getName)
  var bpRunner: BpRulesRunner = null

  def load(uberType: String, startSerial: String, filter: String = "") {
    if (!BpRulesRunner.getUberObjectTypes().contains(uberType)) {
      log.error(uberType + " is not a valid uberObjectType")
    }

    val startLoad = Instant.now
    log.debug(s"load filter parameter=$filter")
    
    // Get high level object from calling into Order Book API
    if (bpRunner == null) {
      bpRunner = BpRulesRunner(ss, dataReaderConfig.configPath)
    }
    bpRunner.loadEvents(startSerial)

    val startGetObject = Instant.now
    val uberObjectDF: DataFrame = bpRunner.getUberObjectsByName(uberType)

    val startJsonFetch = Instant.now
    val finalDf = if (filter != "") uberObjectDF.filter(filter) else uberObjectDF
    highLevelObjectDF = finalDf.toJSON.toDF
  }

  def saveDF(updatedObject: DataFrame): Unit = {
    highLevelObjectDF = updatedObject
  }

  /**
    * Add newly updated JSON Object back to DataFrame
    * @param updatedObjectStr
    */
  def save(updatedObjectStr: String): Unit = {
    import ss.implicits._
    val newDF = List(updatedObjectStr).toDF
    // TODO: we would use this when we return more than one JSON blob.
    // Currently we return only one top blob so we will just override the DF when we save it.
    highLevelObjectDF = newDF
    //highLevelObjectDF = highLevelObjectDF.union(newDF).toDF
  }

  def validate(highLevelObjectStr: String): Unit = {
    if (dataReaderConfig.validationMapper == null)
      return

    // read fields in the config to a validation map
    val validations = dataReaderConfig.validationMapper.getArray("validations")
    val validationRegex = validations.map ( x => {
      val al = x.asInstanceOf[java.util.ArrayList[String]]
      val fieldName = al.get(0)
      val regex = al.get(1)
      (fieldName, regex)
    }).toMap
    log.info(validationRegex)

    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val hloJson = mapper.readValue(highLevelObjectStr, classOf[Map[String, Any]])
    validateMap(None, hloJson, validationRegex)
  }

  private def validateMap(prefix: Option[String], objectMap: Map[String, Any], validationMapRegex: Map[String, String]): Unit = {
    for ((k, v) <- objectMap) {
      // go through all the fields in high level object and
      // check each field with validation map using regex of the map value and field value
      var field = k
      val jsonValue = v
      if (prefix.isDefined) {
        field = prefix.get + "." + k
      }

      if (jsonValue.isInstanceOf[List[_]]) {
        val listOfVal = jsonValue.asInstanceOf[List[Any]]
        for (item <- listOfVal) {
          if (item.isInstanceOf[Map[_, _]]) {
            validateMap(Some(field), item.asInstanceOf[Map[String, Any]], validationMapRegex)
          }
        }
      } else {
        log.info(s"Check $field: $jsonValue with validation mapping file")
        if (validationMapRegex.contains(field)) {
          val regex = validationMapRegex.get(field).get.r
          log.info(s"$field: $jsonValue is trying to match with $regex")
          val found = regex.findFirstIn(jsonValue.asInstanceOf[String])
          if (!found.isDefined)  throw InvalidFormatException(s"Invalid data format for $field: $jsonValue")
        } else {
          throw MissingMappingException(s"There isn't validation mapping for : $field")
        }
      }
    }
  }

  def getHighLevelObjectDF(limit: Int = -1): DataFrame = {
     if (highLevelObjectDF == null) return null
    if (limit == -1) {
      highLevelObjectDF
    } else {
      highLevelObjectDF.limit(limit)
    }
  }

  def getHighLevelObjectJson(limit: Int = -1): String = {
    if (highLevelObjectDF == null) return null
    val startCollectTimer = Instant.now
    val elements = if (limit == -1) {
      highLevelObjectDF.toJSON.select("value").collect().map(_.get(0))
    } else {
      highLevelObjectDF.toJSON.select("value").take(limit).map(_.get(0))
    }
    elements(0).toString
  }

  def getHighLevelObjectsList(limit: Int = -1): Seq[String] = {
    val startSelectTimer = Instant.now

    if (highLevelObjectDF == null) return null

    val elements = if (limit == -1) {
      highLevelObjectDF.select("value")
    } else {
      highLevelObjectDF.select("value").limit(limit)
    }

    val startSeqCollectTimer = Instant.now

    val result = elements.collect().map(_(0).toString).toSeq

    result
  }

}

object DataReader {
  val log: Log = LogFactory.getLog(DataReader.getClass.getName)
  /**
    * Initialize Data reader object to create high level DataFrame based on provided configs and contexts
    * @return DataReader object which we would use to get and update high level object
    */
  def apply(ss: SparkSession, appContext: AppContext): DataReader = {
    val validationConfigName = appContext.appConfig.validationMapperConfigMap.getOrElse("path", "")
    val configPath = appContext.appConfig.configPath
    val validationMapper = if (validationConfigName != "") {
      Dictionary(validationConfigName)
    } else {
      log.warn("No validationConfigPath was defined to validate uber objects")
      null
    }
    val dataReaderConfig = new DataReaderConfig(configPath, validationMapper)
    new DataReader(ss, dataReaderConfig)
  }
}

object UnitTestObject {
  def printMessage(message: String) : String = {
    println(message)
    "pass"
  }
}

