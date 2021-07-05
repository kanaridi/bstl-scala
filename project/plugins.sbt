// resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// addSbtPlugin("no.arktekk.sbt" % "aether-deploy" % "0.23.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.13")

// addSbtPlugin("io.shiftleft" % "sbt-ci-release-early" % "1.2.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.7.0")
