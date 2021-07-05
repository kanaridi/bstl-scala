/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.util

import com.KanariDigital.app.AppType
import org.apache.commons.logging.{Log, LogFactory}


/** zeppelin log utils
  */
object ZeppelinLogUtils {

  val log: Log = LogFactory.getLog(this.getClass.getName)
  
  def logZeppelinError(sparkApplicationId: String, message: String, logType: String = "AppLog"): Unit = {
    log.error(s"""$sparkApplicationId - ${LogTag.ZEPPELIN} -
                |applicationId: $sparkApplicationId, appName: $sparkApplicationId, appType: ${AppType.OB_RULES_APP_TYPE}, logType: $logType,
                |message: $message"""
      .stripMargin.replaceAll("\n", " "))
  }

  def logZeppelinInfo(sparkApplicationId: String, message: String, logType: String = "AppLog"): Unit = {
    log.info(s"""$sparkApplicationId - ${LogTag.ZEPPELIN} -
                |applicationId: $sparkApplicationId, appName: $sparkApplicationId, appType: ${AppType.OB_RULES_APP_TYPE}, logType: $logType,
                |message: $message"""
      .stripMargin.replaceAll("\n", " "))
  }

  def logZeppelinDebug(sparkApplicationId: String, message: String, logType: String = "AppLog"): Unit = {
    log.debug(s"""$sparkApplicationId - ${LogTag.ZEPPELIN} -
                |applicationId: $sparkApplicationId, appName: $sparkApplicationId, appType: ${AppType.OB_RULES_APP_TYPE}, logType: $logType,
                |message: $message"""
      .stripMargin.replaceAll("\n", " "))
  }

  def logZeppelinWarn(sparkApplicationId: String, message: String, logType: String = "AppLog"): Unit = {
    log.warn(s"""$sparkApplicationId - ${LogTag.ZEPPELIN} -
                 |applicationId: $sparkApplicationId, appName: $sparkApplicationId, appType: ${AppType.OB_RULES_APP_TYPE}, logType: $logType,
                 |message: $message"""
      .stripMargin.replaceAll("\n", " "))
  }
}
