/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.KafkaConsumer

trait KafkaTopicLoggerTrait {
  def log(topic: String, kafkaOperationResult: KafkaOperationResult)
}
