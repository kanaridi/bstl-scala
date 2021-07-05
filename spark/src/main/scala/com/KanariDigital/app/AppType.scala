/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app

/** Types of streaming apps
  *
  *  {{
  *  HDFS_STREAMING_APP_TYPE
  *  EVENTHUB_STREAMING_APP_TYPE
  *  KAFKA_STREAMING_APP_TYPE
  *  }}
  */
object AppType {

  val HDFS_STREAMING_APP_TYPE: String = "hdfs_streaming"
  val HDFS_DSTREAM_APP_TYPE: String = "hdfs_dstream"
  val EVENTHUB_STREAMING_APP_TYPE: String = "eventhub_streaming"
  val KAFKA_STREAMING_APP_TYPE: String = "kafka_streaming"
  val OB_FUSION_APP_TYPE: String = "ob_fusion"
  val OB_RULES_APP_TYPE: String = "ob_rules"
  val FLOW_APP_TYPE: String = "flow"
  val ABFS_STREAMING_APP_TYPE: String = "abfs_streaming"
  val NONE_TYPE: String = "none"

}
