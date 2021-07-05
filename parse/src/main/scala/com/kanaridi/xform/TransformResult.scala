/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.xform

case class TransformResult (
  val hbaseRows : List[HBRow],
  val docIDs : Set[String],
  val topic: String
) extends Serializable
