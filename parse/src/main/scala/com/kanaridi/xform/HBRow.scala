/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.xform
import scala.collection.immutable.Map
import java.security.MessageDigest

case class HBRow (
  tableName: String,
  rowKey: String,
  family: String,
  cells: Map[String, String],
  namespace: String,
  deDupKey: String,
  sortKey: String
) extends Serializable
