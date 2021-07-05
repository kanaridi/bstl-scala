/* Copyright (c) 2020 Kanari Digital, Inc. */

import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import Dependencies.dependencyVersion



object Settings {

  val hdpVersion = Option(System.getProperty("hdp")).getOrElse("3.1") //this is deprecated
  val targetSystem = Option(System.getProperty("system")).getOrElse("synapse2.4")
  val log4jVersion = "1.2.17"

  //if (hdpVersion != "3.1") throw new IllegalArgumentException("only hdp 3.1 is supported through the --Dsystem=hdp3.1 parameter")

  println("################################################################")
  println(s"# Building for system '${targetSystem}'")
  println("# sbt --Dsystem=[synapse2.4|hdp3.1|hdp2.6] compile assembly")
  println("#    depracated: --Dhdp param")
  println("################################################################")

  lazy val settings = Seq(
    organization := "com.kanaridi.bstl",
    // version := "0.3.1" + sys.props.getOrElse("buildNumber", default="-fusion"),
    scalaVersion := "2.11.12",
    publishMavenStyle := true,
    publishArtifact in Test := false,
    scalacOptions += "-target:jvm-1.8"
  )

  lazy val testSettings = Seq(
    fork in Test := true,
    parallelExecution in Test := false
  )

  lazy val itSettings = Defaults.itSettings ++ Seq(
    logBuffered in IntegrationTest := false,
    fork in IntegrationTest := true
  )

  lazy val parseSettings = Seq(
    assemblyJarName in assembly := "bstl-" + version.value + "_" + targetSystem + ".jar",
    test in assembly := {},
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(
      includeScala = false,
      includeDependency=false),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case n if n.startsWith("reference.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

  lazy val shcSettings = Seq(
    assemblyJarName in assembly := "shc-" + version.value + "_" + targetSystem + ".jar",
    test in assembly := {},
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(
      includeScala = false,
      includeDependency=false),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case n if n.startsWith("reference.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

  lazy val commonSettings = Seq(
    assemblyJarName in assembly := "common-" + version.value + "_" + targetSystem + ".jar",
    test in assembly := {},
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(
      includeScala = false,
      includeDependency=false),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case n if n.startsWith("reference.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

  lazy val sparkSettings = Seq(
    assemblyJarName in assembly := "kdspark-" + version.value + "_" + targetSystem + ".jar",
    test in assembly := {},
    target in assembly := file(baseDirectory.value + "/../bin/"),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(
      includeScala = false,
      includeDependency=true),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case n if n.startsWith("reference.conf") => MergeStrategy.concat
      case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
      case PathList(ps @ _*) if ps.contains("lz4") => MergeStrategy.first
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("org.scala-lang.modules", "scala-parser-combinators", xs @ _*) => MergeStrategy.last
      case PathList("junit", "junit", xs @ _*) => MergeStrategy.last
      case PathList("ch.qos.logback", "logback-classic", xs @ _*) => MergeStrategy.last
      case PathList("com.typesafe.scala-logging", "scala-logging", xs @ _*) => MergeStrategy.last
      case PathList("org.slf4j", "slf4j-api", xs @ _*) => MergeStrategy.deduplicate
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("org.glassfish.hk2.external", "aopalliance-repackaged", impl @ _*) => MergeStrategy.last
      case PathList("org", "aopalliance", impl @ _*) => MergeStrategy.last
      case PathList("org", "glassfish", impl @ _*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", minlog @ _*) => MergeStrategy.last
      case PathList("org", "slf4j", impl @ _*) => MergeStrategy.last

      case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
      case PathList("javax", "ws", xs @ _*) => MergeStrategy.first
      case PathList("jersey",".", xs @ _*) => MergeStrategy.first
      case PathList("jersey-server",".", xs @ _*) => MergeStrategy.first

      case _ => MergeStrategy.first
    }
  )

}
