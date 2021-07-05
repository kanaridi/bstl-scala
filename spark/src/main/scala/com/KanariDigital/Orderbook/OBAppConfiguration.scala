/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.logging.{Log, LogFactory}

class OBAppConfiguration extends Serializable {

  val log: Log = LogFactory.getLog("OBAppConfiguration")

  var enableDedup: Option[String] = None
  var dedupColFamily: Option[String] = None
  var dedupColName: Option[String] = None
  var dedupLookback: Option[String] = None
  var maxBatchSize: Option[String] = None
  var defaultStorageLevel: Option[String] = None
  var fusionAvroPath: Option[String] = None

  def validationReasons() : List[String] = {
    var reasons = ArrayBuffer.empty[String]
    if (enableDedup.isDefined == false) reasons += "enable dedup is not defined"
    if (enableDedup.isDefined && enableDedup.get == "true") {
      if(dedupColFamily.isDefined == false) reasons += "dedup column family is not defined"
      if(dedupColName.isDefined == false) reasons += "dedup column name is not defined"
      if(dedupLookback.isDefined == false) reasons += "dedup lookback is not defined"
    }
    if (maxBatchSize.isDefined == false) reasons += "max batch size is not defined"
    if (defaultStorageLevel.isDefined == false) reasons += "default storage is not defined"
    return reasons.toList
  }

  def isValid() : Boolean = validationReasons().size == 0

  override def toString() : String = {
    val undef = "Undefined"
    val enableDedupStr = if (enableDedup.isDefined) enableDedup.get.toString() else undef
    val dedupColFamilyStr = if (dedupColFamily.isDefined) dedupColFamily.get.toString() else undef
    val dedupColNameStr = if (dedupColName.isDefined) dedupColName.get.toString else undef
    val dedupLookbackStr = if (dedupLookback.isDefined) dedupLookback.get.toString else undef
    val maxBatchSizeStr = if (maxBatchSize.isDefined) maxBatchSize.get.toString else undef
    val defaultStorageLevelStr = if (defaultStorageLevel.isDefined) defaultStorageLevel.get.toString else undef
    val fusionAvroPathStr = if (defaultStorageLevel.isDefined) defaultStorageLevel.get.toString else undef

    s"""
       | OBAppConfiguration[ enableDedup:                $enableDedupStr
       |                     dedupColFamily:             $dedupColFamilyStr
       |                     dedupColName:               $dedupColNameStr
       |                     dedupLookback:              $dedupLookbackStr
       |                     maxBatchSize:               $maxBatchSizeStr
       |                     defaultStorageLevel:        $defaultStorageLevelStr
       |                     fusionAvroPath:             $fusionAvroPathStr
       | ]
     """.stripMargin
  }
}
