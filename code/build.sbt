/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

lazy val commonDependencies = Seq(
  // Java
  Dependencies.Libraries.config,
  Dependencies.Libraries.slf4j,
  Dependencies.Libraries.log4jOverSlf4j,
  Dependencies.Libraries.kinesisClient,
  Dependencies.Libraries.kinesisConnector,
  Dependencies.Libraries.validator,
  Dependencies.Libraries.nsqClient,
  Dependencies.Libraries.jacksonCobr,
  Dependencies.Libraries.scaffeine,
  Dependencies.Libraries.collectorPayload,
  // Scala
  Dependencies.Libraries.scopt,
  Dependencies.Libraries.scalaz7,
  Dependencies.Libraries.scalazC7,
  Dependencies.Libraries.snowplowTracker,
  Dependencies.Libraries.analyticsSDK,
  Dependencies.Libraries.awsSigner,
  Dependencies.Libraries.pureConfig,
  Dependencies.Libraries.awsSts,
  Dependencies.Libraries.awsCore,
 // Dependencies.Libraries.json4sJackson,
  // Scala (test only)
  Dependencies.Libraries.specs2
)

lazy val buildSettings = Seq(
  organization  := "com.snowplowanalytics",
  name          := "snowplow-stream-loader",
  version       := "0.10.1",
  description   := "Load the contents of a Kinesis stream or NSQ topic to Elasticsearch or Postgres",
  scalaVersion  := "2.12.7",
  scalacOptions := BuildSettings.compilerOptions,
  javacOptions  := BuildSettings.javaCompilerOptions,
  //resolvers     += Resolver.jcenterRepo,
  resolvers += "JCenter" at "https://jcenter.bintray.com/",  // having SSL cert problems with JCenter, support for HTTPS has been deprecated.
  resolvers += "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases",
  shellPrompt   := { _ => "stream-loader> " }
)

//resolvers += "JCenter" at "https://jcenter.bintray.com/" // you can omit if you're planning to use Maven Central


lazy val allSettings = buildSettings ++
  BuildSettings.sbtAssemblySettings ++
  Seq(libraryDependencies ++= commonDependencies)

lazy val root = project.in(file("."))
  .settings(buildSettings)
  .aggregate(core, elasticsearch, postgres, s3)

lazy val core = project
  .settings(moduleName := "snowplow-stream-loader-core")
  .settings(buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(libraryDependencies ++= commonDependencies)

// project dealing with the ES HTTP API
lazy val elasticsearch = project
  .settings(moduleName := "snowplow-stream-loader-elasticsearch")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.elastic4sCore,
    Dependencies.Libraries.elastic4sHttp,
    Dependencies.Libraries.elastic4sTest
  ))
  .dependsOn(core)

// project dealing with Postgres
lazy val postgres = project
  .settings(moduleName := "snowplow-stream-loader-postgres")
  .settings(allSettings)
  //.settings(resolvers ++= Seq( "Maven2 Central Repository" at "http://central.maven.org/maven2/"))
  .settings(libraryDependencies ++= Seq (
  Dependencies.Libraries.commonsDbcp,
  Dependencies.Libraries.postgresql))
  .dependsOn(core)

// project dealing with S3
lazy val s3 = project
  .settings(moduleName := "snowplow-stream-loader-s3-parquet")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq (
     Dependencies.Libraries.hadoopAws,
      Dependencies.Libraries.hadoopCommon,
      Dependencies.Libraries.parquetAvro,
      Dependencies.Libraries.kiteDataCore
    )
  )
  .dependsOn(core)