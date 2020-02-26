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

import sbt._

object Dependencies {
  object V {
    // Java
    val config           = "1.3.3" //"1.3.1"
    val slf4j            = "1.7.5"
    val kinesisClient    = "1.11.2" //"1.7.5"
    val kinesisConnector = "1.3.0"
    val validator        = "2.2.6"
    val elasticsearch    = "6.2.4" //"2.4.5"
    val nsqClient        = "1.2.0" //"1.1.0-rc1"
    val hadoopAws        = "3.1.1"
    val hadoopCommon     = "3.1.1"
    val parquetAvro      = "1.8.3"
    val kiteDataCore     = "1.1.0"
    val commonsDbcp      = "2.5.0"
    val postgresql       = "42.2.5"
    val jacksonCobr      = "2.9.5"
    val scaffeine        = "2.5.0"
    val collectorPayload = "0.0.0"

    // Scala
    val scopt            = "3.7.0" //"3.6.0"
    val scalaz7          = "7.2.22" //"7.2.14"
    val snowplowTracker  = "0.5.0" //"0.3.0"
    val analyticsSDK     = "0.3.0" // "0.2.0"
    val awsSigner        = "0.5.0"
    val elastic4s        = "6.2.4" //"5.4.6"
    val pureConfig       = "0.9.1" // "0.8.0"
    val awsCore          = "1.11.603" // "1.11.446"
    val awsSts           = "1.11.603" // "1.11.446"
    val json4s           = "3.6.5"
    val igluCore         = "0.2.0"
    // Scala (test only)
    val specs2           = "4.1.0" //"3.9.2"
  }

  object Libraries {
    // Java
    val config           = "com.typesafe"            %  "config"                       % V.config
    val slf4j            = "org.slf4j"               %  "slf4j-simple"                 % V.slf4j
    val log4jOverSlf4j   = "org.slf4j"               %  "log4j-over-slf4j"             % V.slf4j
    val kinesisClient    = "com.amazonaws"           %  "amazon-kinesis-client"        % V.kinesisClient
    val kinesisConnector = "com.amazonaws"           %  "amazon-kinesis-connectors"    % V.kinesisConnector
    val validator        = "com.github.fge"          %  "json-schema-validator"        % V.validator
    val elasticsearch    = "org.elasticsearch"       %  "elasticsearch"                % V.elasticsearch
    val nsqClient        = "com.snowplowanalytics"   %  "nsq-java-client"              % V.nsqClient
    val commonsDbcp      = "org.apache.commons"      % "commons-dbcp2"                 % V.commonsDbcp
    val postgresql       =  "org.postgresql"         % "postgresql"                    % V.postgresql
    val jacksonCobr      = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % V.jacksonCobr

    val awsCore          = "com.amazonaws"           % "aws-java-sdk-core"             % V.awsCore exclude("log4j", "log4j")
    val awsSts           = "com.amazonaws"           % "aws-java-sdk-sts"              % V.awsSts exclude("log4j", "log4j")
    val hadoopAws        = "org.apache.hadoop"       % "hadoop-aws"                    % V.hadoopAws exclude("log4j", "log4j") exclude("com.amazonaws","aws-java-sdk-bundle")
    val hadoopCommon     = "org.apache.hadoop"       % "hadoop-common"                 % V.hadoopCommon exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12")
    val parquetAvro      = "org.apache.parquet"      % "parquet-avro"                  % V.parquetAvro
    val kiteDataCore     = "org.kitesdk"             % "kite-data-core"                % V.kiteDataCore  exclude("com.twitter", "parquet-format")

    val collectorPayload = "com.snowplowanalytics"   % "collector-payload-1"           % V.collectorPayload

    // Scala
    val scopt            = "com.github.scopt"        %% "scopt"                        % V.scopt
    val scalaz7          = "org.scalaz"              %% "scalaz-core"                  % V.scalaz7
    val scalazC7         = "org.scalaz"              %% "scalaz-concurrent"            % V.scalaz7
    val snowplowTracker  = "com.snowplowanalytics"   %% "snowplow-scala-tracker"       % V.snowplowTracker
    val analyticsSDK     = "com.snowplowanalytics"   %% "snowplow-scala-analytics-sdk" % V.analyticsSDK  //excludeAll ExclusionRule(organization = "com.amazonaws")
    val awsSigner        = "io.ticofab"              %% "aws-request-signer"           % V.awsSigner
    val pureConfig       = "com.github.pureconfig"   %% "pureconfig"                   % V.pureConfig
    val elastic4sCore    = "com.sksamuel.elastic4s"  %% "elastic4s-core"               % V.elastic4s exclude("org.slf4j", "slf4j-log4j12")
    val elastic4sHttp    = "com.sksamuel.elastic4s"  %% "elastic4s-http"               % V.elastic4s exclude("org.slf4j", "slf4j-log4j12")
    val json4sJackson    = "org.json4s"              %% "json4s-jackson"               % V.json4s
    val igluCore         = "com.snowplowanalytics"   %% "iglu-core"                    % V.igluCore
    val scaffeine        = "com.github.blemale"      %% "scaffeine"                    % V.scaffeine

    // Scala (test only)
    val specs2           = "org.specs2"              %% "specs2-core"                  % V.specs2       % "test"
    val elastic4sTest    = "com.sksamuel.elastic4s"  %% "elastic4s-embedded"           % V.elastic4s    % "test"
  }
}
