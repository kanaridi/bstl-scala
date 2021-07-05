scalaVersion := "2.11.12"

scalacOptions += "-Y log-classpath"

import Dependencies._
import Settings._

cleanFiles += baseDirectory.value / "bin"

fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms1024M", "-Xmx2048M")

lazy val common = (project in file("common")).
  settings(Settings.settings: _*).
  settings(Settings.commonSettings: _*).
  settings(libraryDependencies ++= commonDependencies)

lazy val shc = (project in file("shc")).
  settings(Settings.settings: _*).
  settings(Settings.shcSettings: _*).
  settings(libraryDependencies ++= shcDependencies)

lazy val parse = (project in file("parse")).
  settings(Settings.settings: _*).
  settings(Settings.parseSettings: _*).
  settings(libraryDependencies ++= parseDependencies).
  dependsOn(common)

lazy val spark = (project in file("spark")).
  settings(Settings.settings: _*).
  settings(Settings.sparkSettings: _*).
  settings(libraryDependencies ++= sparkDependencies).
  settings(dependencyOverrides ++= sparkDependencyOverrides).
  dependsOn(common, parse, shc)


configs(Test)

