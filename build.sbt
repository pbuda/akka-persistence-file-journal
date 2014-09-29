organization := "eu.piotrbuda"

name := "akka-persistence-file-journal"

scalaVersion := "2.11.2"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:postfixOps")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.5",
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
  "commons-codec" % "commons-codec" % "1.9",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.typesafe.akka" %% "akka-persistence-tck-experimental" % "2.3.5" % "test"
)

