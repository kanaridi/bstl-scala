/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


import java.util.Calendar
import java.text.SimpleDateFormat


/**
  *
  */
class EventSink(spark: org.apache.spark.sql.SparkSession,
  name: String,
  upstream: Table,
  prefetchFrom: String,
  prefetchTo: String,
  save: Boolean,
  catchUpIncrement: String,
  recheckPause: Long,
  path: String)
    extends Sink(spark, name, upstream,prefetchFrom, prefetchTo, save, catchUpIncrement, recheckPause)
{

}

