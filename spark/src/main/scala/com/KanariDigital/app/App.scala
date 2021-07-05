/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}

/** abstract base class for all apps */
abstract class App(appName: String, appType: String)
    extends java.io.Serializable {

  val appTypeStr:String = AppType.NONE_TYPE

  override def toString: String = {
    s"appName: $appName [$appType]"
  }

  def runApp(appContext: AppContext, isLocal: Boolean): Unit

}
