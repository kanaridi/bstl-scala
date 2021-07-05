/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import java.time.Duration

import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.functions._


/**
  * returns DataFrame from eventhub data
  */
case class EventhubTable(val spark: org.apache.spark.sql.SparkSession,
  val namespace: String,
  val name: String,
  val sasKeyName: String,
  val sasKey: String,
  val options: Map[String, String]
)
    extends Table
{

  override def all() = {

    // println(s"EventhubSource: name: $name, namespace: $namespace, sasKey: $sasKey, sasKeyName: $sasKeyName")
    // println(s"EventhubSource: options are is $options")

    val connectionString = ConnectionStringBuilder()
      .setNamespaceName(namespace)
      .setEventHubName(name)
      .setSasKeyName(sasKeyName)
      .setSasKey(sasKey)
      .build

    //FIXME: keep track of offsets/enqueuedTime
    val autoOffsetReset = options.get("autoOffsetReset").get
    val eventPos: EventPosition =
      if (autoOffsetReset == "smallest" || autoOffsetReset == "earliest") {
        EventPosition.fromStartOfStream
      } else {
        EventPosition.fromEndOfStream
      }

    val consumerGroup = options.get("consumerGroup").get

    val maxRatePerPartition = options.get("maxRatePerPartition").get.toInt
    val receiverTimeout = options.get("receiverTimeout").get.toInt
    val operationTimeout = options.get("operationTimeout").get.toInt

    val ehConf: EventHubsConf = EventHubsConf(connectionString)
      .setConsumerGroup(consumerGroup)
      .setStartingPosition(eventPos)
      .setMaxRatePerPartition(maxRatePerPartition)
      .setReceiverTimeout(Duration.ofSeconds(receiverTimeout))
      .setOperationTimeout(Duration.ofSeconds(operationTimeout))

    val raw = spark
      .read
      //.format("eventhubs")
      .format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider")
      .options(ehConf.toMap)
      .load()

    val df = raw.select(col("body") cast "string")
    val jsonRdd = df.rdd.map(row => row.getAs("body").toString)

    // convert to a df
    val ldf = spark.read.json(jsonRdd)

    println(s"EventhubSource: read ${ldf.count} rows ...")
    ldf.show(20)

    Some(ldf)
  }

}
