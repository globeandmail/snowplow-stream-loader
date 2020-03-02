//Create a fat JAR of your project with all of its dependencies.
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
//it build application packages in native formats.
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.25")
//Generates Scala source from your build definitions.
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
// to generate dependency graphs
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
//Code formatter for Scala
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.0")
// Scala Style SBT Plugin
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
// Scapegoat is a Scala static code analyzer
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.0.9")
// Code Coverage Plugin
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
// Plugin to check code stats. Run sbt stats.
addSbtPlugin("com.orrsella" % "sbt-stats" % "1.0.7")
