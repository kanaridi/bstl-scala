/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.KafkaConsumer

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.{AuthorizationException, WakeupException}

abstract class KafkaOperationResult {
  def isSuccess : Boolean
}

abstract class KafkaOperationFailure(val ex : Exception) extends KafkaOperationResult {
  override def isSuccess: Boolean = {
    false
  }

  override def toString(): String = {
    val message = ex.toString
    s"Kafka Operation Failure: $message"
  }
}

case class KafkaOperationSuccessful(val offsetAndMetadata: OffsetAndMetadata) extends  KafkaOperationResult {
  override def isSuccess : Boolean = {
    true
  }
}

case class KafkaEmptyOffsetResult() extends KafkaOperationResult {
  override def isSuccess: Boolean = false
  override def toString() = "Offset is empty."
}

case class KafkaAuthorizationFailure(ae: AuthorizationException) extends KafkaOperationFailure(ae) {}
case class KafkaWakeupException(wakeupException: WakeupException) extends KafkaOperationFailure(wakeupException) {}
case class KafkaOtherException(kafkaException: KafkaException ) extends KafkaOperationFailure(kafkaException) {}
case class KafkaUnknownException(u : Exception) extends KafkaOperationFailure(u) {}



