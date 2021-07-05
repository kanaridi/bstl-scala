/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.datasource.eventhub

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.eventhubs.EventHubsString
import org.apache.spark.eventhubs.rdd.{OffsetRange => EventHubOffsetRange}
import org.apache.spark.eventhubs.NameAndPartition

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.immutable.{Map => ScalaMap}

/** eventhub offset specification
  * @param appName - application these offset belong to
  * @param batchTime - batchTime these offsets pertain toarray
  * @offsetRange - {{org.apache.spark.eventhubs.rdd.OffsetRange}}
  */
class EventHubOffsetSpec(

  val appName: String,
  val batchTime: String,
  val offsetRange: EventHubOffsetRange) extends Serializable {

  override def toString(): String = {
    s"appName: $appName, batchTime: $batchTime, offsetRange: $offsetRange"
  }

}

/** utility methods to convert eventhub offset specification into json or convert
  * json representation into a {{EventHubOffsetSpec}} object
  */
object EventHubOffsetSpec {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  /** convert {{EventHubOffSetSpec}} to a json representation 
    * @param offsetSpec EventHubOffsetSpec
    * @return json representation of EventHubOffsetSpec
    */
  def toJSONString(offsetSpec: EventHubOffsetSpec): String = {

      val offsetMap = Map(
        "appName" -> offsetSpec.appName,
        "batchTime" -> offsetSpec.batchTime,
        "topic" -> offsetSpec.offsetRange.nameAndPartition.ehName,
        "partition" -> offsetSpec.offsetRange.nameAndPartition.partitionId.toString,
        "fromOffset" -> offsetSpec.offsetRange.fromSeqNo.toString,
        "toOffset" -> offsetSpec.offsetRange.untilSeqNo.toString)

      //parse map as json
      val jsonCellsMapper = new ObjectMapper()
      jsonCellsMapper.registerModule(DefaultScalaModule)
      val offsetRangeJson = jsonCellsMapper.writeValueAsString(offsetMap)
      offsetRangeJson
  }

  /** convert json representation of {{EventHubOffsetSpec}} to a Map */
  def jsonSpecToMap(jsonSpecStr: String): ScalaMap[String, String] = {
    //json to map
    val jsonMapper = new ObjectMapper
    jsonMapper.registerModule(DefaultScalaModule)
    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val scalaMap = jsonMapper.readValue(jsonSpecStr, classOf[ScalaMap[String,String]])
    scalaMap
  }

  def jsonSpecToEventHubOffsetSpec(jsonSpecStr: String): EventHubOffsetSpec = {

    //json to map
    val scalaMap = jsonSpecToMap(jsonSpecStr)

    val appName = scalaMap("appName")
    val batchTime = scalaMap("batchTime")

    //offset range
    val topic = scalaMap("topic")
    val partition = scalaMap("partition").toInt
    val nameAndPartition = new NameAndPartition(topic, partition)


    val fromOffset = new EventHubsString(scalaMap("fromOffset")).toSequenceNumber
    val toOffset = new EventHubsString(scalaMap("toOffset")).toSequenceNumber

    val eventHubOffsetRange = new EventHubOffsetRange(nameAndPartition, fromOffset, toOffset, None)

    val eventHubOffsetSpec = new EventHubOffsetSpec(appName, batchTime, eventHubOffsetRange)

    eventHubOffsetSpec
  }
}
