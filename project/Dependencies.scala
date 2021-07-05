/* Copyright (c) 2020 Kanari Digital, Inc. */

import sbt._
import Settings.{targetSystem, hdpVersion}

object Dependencies {
  val hdp = hdpVersion //this should be deprecated
  val sys = targetSystem

  var sparkVersion = 0.0
  var hbaseVersion = 0.0

  if (sys == "synapse2.4") {
    sparkVersion = 2.4
    hbaseVersion = 2.0
  } else if (sys == "hdp3.1" || hdp == "3.1") {
    sparkVersion = 2.2
    hbaseVersion = 2.0
  } else if (sys == "hdp2.6" || hdp == "2.6") {
    sparkVersion = 2.1
    hbaseVersion = 1.1
  }


  lazy val dependencyVersion = new {
    var scalaTest  = "3.0.0-M7"
    var scalaCheck = "1.13.4"
    var Scalatra = "2.6.5"
    var spark = ""
    var hbase = ""
    var sparkTestingBase = ""
    var hadoop = "2.5.1"
    var eventHubSparkConnector = ""
    var log4j = "1.2.17"
    var eventHub = "2.3.0"
    val sparkAvro = "4.0.0" //This is used by BstPackageSettings.scala
    val jython = "2.7.2b2"
  }

  println("building for spark version " + sparkVersion)
  if (sparkVersion == 2.4) {
    dependencyVersion.spark = "2.4.0"
    dependencyVersion.sparkTestingBase = "2.4.0_0.12.0"
    dependencyVersion.eventHubSparkConnector = "2.3.13"
  } else if (sparkVersion == 2.2) {
    dependencyVersion.spark = "2.2.0"
    dependencyVersion.sparkTestingBase = "2.2.0_0.12.0"
    dependencyVersion.eventHubSparkConnector = "2.3.13"
  } else if (sparkVersion == 2.1) {
    dependencyVersion.spark = "2.1.0"
    dependencyVersion.sparkTestingBase = "2.1.3_0.14.0"
    dependencyVersion.eventHubSparkConnector = "2.2.10"
  } else {
    throw new IllegalArgumentException(s"unknown configuration spark '${sparkVersion}' for system '${sys}'")
  }

  println("building for hbase version " + hbaseVersion)
  if (hbaseVersion == 2.0) {
    dependencyVersion.hbase = "2.0.2"
  } else if (hbaseVersion == 1.1) {
    dependencyVersion.hbase = "1.1.2"
  } else {
    throw new IllegalArgumentException(s"unknown configuration hbase '${hbaseVersion}' for system '${sys}'")
  }


  val library = new  {
    val test  = "org.scalatest" %% "scalatest" % dependencyVersion.scalaTest % Test
    val scalaMockTest = "org.scalamock" %% "scalamock" % "4.1.0" % Test
    val mockitoTest = "org.mockito" %% "mockito-scala" % "1.3.0" % Test
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % dependencyVersion.sparkTestingBase % Test
    val check = "org.scalacheck" %% "scalacheck" % dependencyVersion.scalaCheck % Test
    val commonsLogging = "commons-logging" % "commons-logging" % "1.2"
    val csvParserTest = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.10.1" % Test
    val combinator =  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.1"
    val json = "com.googlecode.json-simple" % "json-simple" % "1.1"
    val jacksonCore = "com.fasterxml.jackson.core"  % "jackson-core" % "2.10.1"
    val jackson = "com.fasterxml.jackson.core"  % "jackson-databind" % "2.10.1"
    val jacksonScala = "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.1"
    val json4s =   "org.json4s"   %% "json4s-jackson" % "3.5.2"
    val json4sNative = "org.json4s" %% "json4s-native" %  "3.7.0-M1"
    val jython = "org.python" % "jython-standalone" % dependencyVersion.jython % Provided
    val hadoopCommon =  "org.apache.hadoop" % "hadoop-common" % dependencyVersion.hadoop % Provided
    val hadoopClient = "org.apache.hadoop" % "hadoop-client" % dependencyVersion.hadoop % Provided
    val hadoopHDFS = "org.apache.hadoop" % "hadoop-hdfs" % dependencyVersion.hadoop % Provided
    val hadoopAnnotations = "org.apache.hadoop" % "hadoop-annotations" % dependencyVersion.hadoop % Provided
    val hadoop_aws  = "org.apache.hadoop"%  "hadoop-aws"       % "3.1.1" % Provided
    val hbase = "org.apache.hbase" % "hbase" % dependencyVersion.hbase % Provided
    val hbaseClient = "org.apache.hbase" % "hbase-client" % dependencyVersion.hbase % Provided
    val hbaseCommon = "org.apache.hbase" % "hbase-common" % dependencyVersion.hbase % Provided
    val hbaseCommonNP = "org.apache.hbase" % "hbase-common" % dependencyVersion.hbase
    val hbaseMapReduce = "org.apache.hbase" % "hbase-mapreduce" % dependencyVersion.hbase % Provided
    val hbaseServer = "org.apache.hbase" % "hbase-server" % dependencyVersion.hbase % Provided
    val hbaseHadoopCompat = "org.apache.hbase" % "hbase-hadoop-compat" % dependencyVersion.hbase % Provided
    val kafkaClients = "org.apache.kafka" % "kafka-clients" % "2.1.0"
    val msSqlJdbc = "com.microsoft.sqlserver" % "mssql-jdbc" % "8.2.2.jre8"
    val scopt = "com.github.scopt" %% "scopt" % "4.0.0-RC2"
    val sparkCore = "org.apache.spark" %% "spark-core" % dependencyVersion.spark % Provided
    val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka-0-10" % dependencyVersion.spark
    val sparkTags = "org.apache.spark" %% "spark-tags" % dependencyVersion.spark % Provided
    val sparkHive = "org.apache.spark" %% "spark-hive" % dependencyVersion.spark % Provided
    val sparkSQL = "org.apache.spark" %% "spark-sql" % dependencyVersion.spark % Provided
    val sparkStreaming = "org.apache.spark" %% "spark-streaming" % dependencyVersion.spark % Provided
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
    val slf4j = "org.slf4j" % "slf4j-api" % "1.7.25"
    val log4j = "log4j" % "log4j" % dependencyVersion.log4j
    val log4jAPI = "org.apache.logging.log4j" % "log4j-api" % "2.11.2"
    val log4jCore = "org.apache.logging.log4j" % "log4j-core" % "2.11.2"
    val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
    val liftJson =  "net.liftweb" %% "lift-json" % "2.6-M4"
    val eventHubSparkConnector = "com.microsoft.azure" %% "azure-eventhubs-spark" % dependencyVersion.eventHubSparkConnector % Provided
    val eventHub = "com.microsoft.azure" % "azure-eventhubs" % dependencyVersion.eventHub
    val scalatra = "org.scalatra" %% "scalatra" % dependencyVersion.Scalatra
    val scalatraJson = "org.scalatra" %% "scalatra-json" % dependencyVersion.Scalatra
    val scalatraSwaggr = "org.scalatra" %% "scalatra-swagger"  % dependencyVersion.Scalatra
    val scalatraTest = "org.scalatra" %% "scalatra-scalatest" % dependencyVersion.Scalatra % Test
    val jetty = "org.eclipse.jetty" % "jetty-webapp" % "9.4.19.v20190610"
    val servlet = "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided"
  }

  val parseDependencies: Seq[ModuleID] = Seq(
    library.test,
    library.sparkTestingBase,
    library.scalaMockTest,
    library.hbaseCommon,
    library.combinator,
    library.check,
    library.jackson,
    library.jacksonScala,
    library.sparkCore,
    library.sparkSQL,
    library.eventHubSparkConnector,
    library.eventHub
  )

  val shcDependencies: Seq[ModuleID] = Seq(
    library.hbase,
    library.hbaseClient,
    library.hbaseCommon,
    library.hbaseServer,
    library.hbaseHadoopCompat,
    library.hadoopCommon,
    library.sparkCore,
    library.sparkStreaming,
    library.sparkStreamingKafka,
    library.sparkSQL,
    library.scalaLogging,
    library.hbaseMapReduce
  )

  val sparkDependencies: Seq[ModuleID] = Seq(
    library.test,
    library.sparkTestingBase,
    library.scalaMockTest,
    library.mockitoTest,
    library.csvParserTest,
    library.hadoopCommon,
    library.hbaseServer,
    library.hbase,
    library.hbaseClient,
    library.hbaseCommon,
    library.hbaseHadoopCompat,
    library.kafkaClients,
    library.sparkCore,
    library.sparkStreaming,
    library.sparkStreamingKafka,
    library.sparkSQL,
    library.msSqlJdbc,
    library.scalaLogging,
    library.scopt,
    library.json,
    library.json4sNative,
    library.json4s,
    library.eventHubSparkConnector,
    library.jython,
    library.eventHub,
    library.hbaseMapReduce
  )

  val sparkDependencyOverrides: Seq[ModuleID] = Seq(
    library.jacksonCore,
    library.jackson,
    library.jacksonScala
  )

  val commonDependencies: Seq[ModuleID] = Seq(
    library.jackson,
    library.hadoopCommon,
    library.json4s
  )

}
