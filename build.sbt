import Dependencies._

name := "Scala/R/Python interworking"

version := "0.9"

organization := "org.programming-scala"

resolvers +=
  ("Caeneus" at "http://caeneus.fritz.box:8081/repository/caeneus-3/").withAllowInsecureProtocol(true)

scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-async"     % "0.9.6",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "org.scala-lang.modules" %% "scala-xml"       % "1.2.0",
  "org.scala-lang"          % "scala-reflect"   % scalaVersion.value,
  "org.slf4j"               % "slf4j-api"       % "1.7.36",
  "ch.qos.logback"          % "logback-classic" % "1.2.12",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.specs2" %% "specs2-core" % "4.6.0" % "test",
  // JUnit is used for some Java interop. examples. A driver for JUnit:
  "junit"                   % "junit-dep"       % "4.10"   % "test",
  "com.novocode"            % "junit-interface" % "0.10"   % "test",
  "commons-io" 		    % "commons-io" % "2.11.0",
  scalaTest,
  scalaCheck
)

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
