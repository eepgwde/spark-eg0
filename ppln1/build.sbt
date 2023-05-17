import Dependencies._

name := "Pipeline 1"

version := "0.0.1-SNAPSHOT"

organization := "org.artikus"

resolvers +=
  ("Caeneus" at "http://caeneus.fritz.box:8081/repository/caeneus-3/").withAllowInsecureProtocol(true)

scalaVersion := "2.12.17"

// "-Yinline-warnings",
// Nice, but hard to eliminate these warnings: "-Ywarn-value-discard")
//  "-optimise",
// "-Xlint",

val scalacOptions0 = Seq(
  "-encoding", "UTF-8",
  "-deprecation", "-unchecked", "-feature",
  "-language:experimental.macros",
  "-Xlint", "-Ywarn-unused-import", "-Ywarn-unused:imports",
  "-Ywarn-infer-any")

val scalacOptions1 = Seq(
  "-encoding", "UTF-8",
  "-language:experimental.macros",
  "-Ypartial-unification",
  "-deprecation", "-unchecked", "-feature",
  "-language:experimental.macros" )

scalacOptions ++= scalacOptions1

javacOptions  ++= Seq(
  "-Xlint:unchecked", "-Xlint:deprecation") // Java 8: "-Xdiags:verbose")

libraryDependencies ++= Seq(
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "4.4.2",

  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.4.0" % "provided",

  "org.scala-lang.modules" %% "scala-xml"       % "2.1.0",
  "org.scala-lang"          % "scala-reflect"   % scalaVersion.value,
  "ch.qos.logback"          % "logback-classic" % "1.4.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.specs2" %% "specs2-core" % "4.6.0" % "test",
  // JUnit is used for some Java interop. examples. A driver for JUnit:
  "junit"                   % "junit-dep"       % "4.10"   % "test",
  "com.novocode"            % "junit-interface" % "0.10"   % "test",
  "commons-io" 		    % "commons-io" % "2.11.0",
  scalaTest,
  scalaCheck
)

Compile / doc / scalacOptions ++= Seq("-groups", "-implicits")

Compile / doc / javacOptions ++= Seq("-notimestamp", "-linksource")