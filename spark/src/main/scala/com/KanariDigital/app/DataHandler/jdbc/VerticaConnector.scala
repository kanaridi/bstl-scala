/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler.jdbc

// NOTE: Depend on version of Vertica we would need a specific .jar
// Currently we are using vertica-jdbc-9.2.1-0.jar for Vertica 9.2.x
// Make sure we have the .jar file in spark submit job
case class VerticaConfig(username: String, password: String, database: String, hostname: String, port: String)

object VerticaConnector {
  /*
  val log: Log = LogFactory.getLog(this.getClass.getName)

  def getTableDF(bstConfigPath: String, connectionName: String, sqlContext: SQLContext, table: String): Option[DataFrame] = {
    val verticaConfig = getConfig(bstConfigPath, connectionName)
    if (verticaConfig.isDefined) {
      val config = verticaConfig.get
      val connectionProperties = new Properties()
      connectionProperties.put("user", config.username)
      connectionProperties.put("password", config.password)
      Class.forName(Constants.VerticaJDBCDriver)

      val url = s"jdbc:vertica://${config.hostname}:${config.port}/${config.database}"

      val df = sqlContext.read.jdbc(url, table, connectionProperties)
      Some(df)
    } else {
      None
    }
  }

  def getConnection(bstConfigPath: String, connectionName: String): Option[Connection] = {
    getConnection(bstConfigPath, "vertica.json", connectionName)
  }

  def getConnection(bstConfigPath: String, configPath: String, connectionName: String): Option[Connection] = {
    val verticaConfig = getConfig(bstConfigPath, configPath, connectionName)
    if (verticaConfig.isDefined) {
      val config = verticaConfig.get
      val connectionProperties = new Properties()
      connectionProperties.put("user", config.username)
      connectionProperties.put("password", config.password)
      Class.forName(Constants.VerticaJDBCDriver)

      val url = s"jdbc:vertica://${config.hostname}:${config.port}/${config.database}"

      val myDbConn = DriverManager.getConnection(url, connectionProperties)
      Some(myDbConn)
    } else {
      None
    }
  }

  /**
    * Get the right vertica config from connection name provided by user
    * @param connectionName connection name so we would use to call into API to get vertica config content
    * @return
    */
  private def getConfig(bstConfigPath: String, connectionName: String): Option[VerticaConfig] = {
    getConfig(bstConfigPath, connectionName)
  }

  /**
    * get vertica config object from config path
    * @param configPath path of vertica config inside Kanari user space
    * @param connectionName connection name so we would use to call into API to get vertica config content
    * @return
    */
  private def getConfig(bstConfigPath: String, connectionName: String): Option[VerticaConfig] = {
    //val configContent = ConfigHelper.getHDFSFile(bstConfigPath, configPath)
    val context = AppContext(bstConfigPath, Some(Constants.AppName), Constants.HadoopResources)
    context.appConfig.rulesConfigMap.getOrElse(connectionName, "")
    if (configContent.nonEmpty) {
      log.info(configContent.get)
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      val verticaOption = mapper.readValue(configContent.get, classOf[VerticaConfig])
      Some(verticaOption)
    } else {
      None
    }
  }
*/
}
