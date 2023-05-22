import Dependencies._

name := "Pipeline 1"

version := "0.0.2-SNAPSHOT"

organization := "org.artikus"

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
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "4.4.2" % "provided",
  "org.postgresql" % "postgresql" % "42.2.15" % "provided",

//   "org.apache.spark" %% "spark-core" % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.4.0" % "provided",

  "org.apache.hadoop" % "hadoop-common" % "3.3.5" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "3.3.5" % "provided",

  "org.scala-lang.modules" %% "scala-xml"       % "2.1.0" % "provided",
  "org.scala-lang"          % "scala-reflect"   % scalaVersion.value % "provided",
  "ch.qos.logback"          % "logback-classic" % "1.4.7" % "provided",

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.specs2" %% "specs2-core" % "4.19.2" % "test",
  scalaTest
)

Compile / doc / scalacOptions ++= Seq("-groups", "-implicits")

Compile / doc / javacOptions ++= Seq("-notimestamp", "-linksource")

lazy val app = (project in file("."))
  .settings(
    assembly / assemblyJarName := "ppln1-fatjar-0.0.2-SNAPSHOT.jar"
  )

publishTo := {
  val caeneus = "http://caeneus.fritz.box:8081/repositories/"
  if (version.value.endsWith("SNAPSHOT"))
    Some(("caeneus-snapshots" at caeneus + "caeneus-2").withAllowInsecureProtocol(true))
  else
    Some(("caeneus-releases" at caeneus + "caeneus-1").withAllowInsecureProtocol(true))
}

credentials += Credentials(Path.userHome / ".sbt" / "caeneus.properties")

ThisBuild / assemblyMergeStrategy := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

/*
val example = {
  val cp: Seq[File] = (fullClasspath in Runtime).value.files
}

 */
