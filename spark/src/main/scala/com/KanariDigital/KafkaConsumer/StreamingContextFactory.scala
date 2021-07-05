/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.KafkaConsumer

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingContextFactory {
  val log: Log = LogFactory.getLog("StreamingContextFactory")
  val DEFAULT_BATCH_DURATION = 10L

  def createStreamingContext(kafkaConfiguration: KafkaConfiguration,
                             sc: SparkContext): StreamingContext = {

    val duration = kafkaConfiguration.batchDuration
    val sec = duration.getOrElse(DEFAULT_BATCH_DURATION)
    if (duration.isDefined == false) {
      log.info(s"Kafka Configuration does not define a batch duration, using default of $DEFAULT_BATCH_DURATION seconds.")
    }
    log.info(s"Creating Spark Streaming Context with batch duration of $sec seconds")
    val context = new StreamingContext(sc, Seconds(sec))

    var checkpointDir = kafkaConfiguration.streamingContextCheckpoint
    if (checkpointDir.isDefined) {
      var cp = checkpointDir.get
      log.info(s"Setting Streaming Context checkpoint directory to: $cp")
      context.checkpoint(cp)
    }
    context
  }
}
