import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.1.0" % "test" ;
  lazy val scalaCheck = "org.scalacheck"         %% "scalacheck"      % "1.14.1" % "test" ;
}
