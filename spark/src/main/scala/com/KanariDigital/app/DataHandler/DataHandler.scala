/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler

import com.KanariDigital.Orderbook.BpRulesRunner
import com.KanariDigital.app.AppContext
import org.apache.spark.sql.SparkSession

class DataHandler(ss: SparkSession, appContext: AppContext) {

  def getDataReader(): DataReader = {
    DataHandler.dataReader.get
  }

  def getDataWriter(): DataWriter = {
    DataHandler.dataWriter.get
  }

}

object DataHandler {
  var dataReader: Option[DataReader] = Option.empty
  var dataWriter: Option[DataWriter] = Option.empty
  var uberObjectType: Option[Seq[String]] = Option.empty

  def apply(ss: SparkSession, configPath: String): DataHandler = {
    val appContext = AppContext(configPath, Some(Constants.AppName), Constants.HadoopResources)
    apply(ss, appContext)
  }

  def apply(ss: SparkSession, appContext: AppContext): DataHandler = {
    dataReader = Option(DataReader.apply(ss, appContext))
    dataWriter = Option(DataWriter.apply(ss, appContext))
    uberObjectType = Option(BpRulesRunner.getUberObjectTypes())

    new DataHandler(ss, appContext)
  }

  def getUberObjectTypes(): Seq[String] = {
    return BpRulesRunner.getUberObjectTypes()
  }
}
