/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler

import com.KanariDigital.Orderbook.{UberObject, UberObjectType}
import com.KanariDigital.app.{AppConfig, AppContext}
import org.apache.commons.logging.{Log, LogFactory}


class ConfigHelper {
  val log: Log = LogFactory.getLog(this.getClass.getName)
  var bstConfigPath:String = ""
  var appConfig:AppConfig = null
  var uberType:UberObjectType = null
  var dependencyStoragePath = ""
  var testResultStoragePath = ""

  def setAppConfig(config:AppConfig) {appConfig = config}
  def setUberObjectType(uberObjType:UberObjectType) {uberType = uberObjType}

  def getRulesConfigSection(): Map[String,String] = {
    if (uberType != null) {
      appConfig.uberRulesConfigMap.getOrElse(uberType.toString, Map[String, String]())
    } else {
      appConfig.rulesConfigMap
    }
  }

  def getDependencyStoragePath(): String = {
    if (dependencyStoragePath != "") return dependencyStoragePath

    val foundRuleMap  = getRulesConfigSection()
    if (foundRuleMap.isEmpty) {
      log.error(s"uberObjectType ${uberType.toString} not found in config")
      throw new IllegalArgumentException(s"uberObjectType ${uberType.toString} not found in config")
    }

    dependencyStoragePath = foundRuleMap.getOrElse("dependency-path", Constants.KanariUserStorageDefaultPath)
    dependencyStoragePath
  }

  def getTestResultStoragePath(): String = {
    if (testResultStoragePath != "") return testResultStoragePath

    val foundRuleMap  = getRulesConfigSection()
    if (foundRuleMap.isEmpty) {
      log.error(s"uberObjectType ${uberType.toString} not found in config")
      throw new IllegalArgumentException(s"uberObjectType ${uberType.toString} not found in config")
    }
    testResultStoragePath = foundRuleMap.getOrElse("results-path", Constants.KanariTestResultStorageDefaultPath)
    testResultStoragePath
  }

  def getDeploymentRootPath(): String = {
    appConfig.ruletestConfigMap("root-path")
  }
}

object ConfigHelper {
  val log: Log = LogFactory.getLog(this.getClass.getName)
  var currentHelper:ConfigHelper = null
  def fromConfigPath(configFile: String, uberTypeStr:String = "", forceOverwrite:Boolean = false) : ConfigHelper = {
    if (currentHelper == null || forceOverwrite) {
      fromAppConfig(AppContext(configFile, Some(Constants.AppName), Constants.HadoopResources).appConfig, uberTypeStr, forceOverwrite)
    } else {
      log.warn("ConfigHelper was already configured")
      currentHelper
    }
  }

  def fromAppConfig(appConfig: AppConfig, uberTypeStr:String = "", forceOverwrite:Boolean = false) : ConfigHelper = {
    if (currentHelper == null || forceOverwrite) {
      currentHelper = new ConfigHelper
      currentHelper.setAppConfig(appConfig)
      if (uberTypeStr != "") currentHelper.setUberObjectType(UberObject(uberTypeStr))
    } else {
      log.warn("ConfigHelper was already configured")
    }
    currentHelper
  }

  //def createHelperFromContext(appContext: AppContext, uberTypeStr:String = "") : ConfigHelper = {

  //}

  def getConfigHelper(): ConfigHelper = {
    if (currentHelper == null) {
      log.error("getConfigHelper called before creating a configHelper object")
      throw new RuntimeException("getConfigHelper called before creating a configHelper object")
    }
    currentHelper
  }
}
