/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler

object Constants {
  val HadoopResources = Array(
    "/usr/hdp/current/hadoop-client/conf/hdfs-site.xml",
    "/usr/hdp/current/hadoop-client/conf/core-site.xml")

  val DefaultKanariConfigPath = "/kanari/config/ob/fusion-config.json"
  val KanariUserStorageDefaultPath = "abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules_dev/dependencies/"
  val KanariTestResultStorageDefaultPath = "abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules_dev/zeppelin_results_output/"

  val ProcessRepoMileStoneTemplate = "rules"
  val ResultsTemplate = "results"

  val VerticaJDBCDriver = "com.vertica.jdbc.Driver"

  val AppName = "ob_rules"
}
