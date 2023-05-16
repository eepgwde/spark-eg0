// addSbtPlugin("org.ensime" % "sbt-ensime" % "2.5.2")
// addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.2")
resolvers += Resolver.sonatypeRepo("snapshots")
addSbtPlugin("ch.epfl.scala" % "sbt-bloop" % "1.3.4+298-2c6ff971")

// See build.sbt scalafix only works for 2.12.8
// addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.4")
