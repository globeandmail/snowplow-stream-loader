/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package model

import java.text.SimpleDateFormat

import scala.util.Try

package Config {

  import utils.JsonUtils

  sealed trait StreamType
  case object Good extends StreamType
  case object Bad extends StreamType
  case object PlainJson extends StreamType

  case class SinkConfig(good: String, bad: String)
  case class AWSConfig(accessKey: String, secretKey: String, arnRole: Option[String], stsRegion: Option[String])

  sealed trait QueueConfig
  final case class Nsq(
    channelName: String,
    host: String,
    port: Int,
    lookupPort: Int
  ) extends QueueConfig
  final case class Kinesis(
    initialPosition: String,
    initialTimestamp: Option[String],
    initialRCU: Option[Int],
    initialWCU: Option[Int],
    maxRecords: Long,
    region: String,
    appName: String,
    customEndpoint: Option[String],
    dynamodbCustomEndpoint: Option[String]
  ) extends QueueConfig {
    val timestampEither = initialTimestamp
      .toRight("An initial timestamp needs to be provided when choosing AT_TIMESTAMP")
      .right
      .flatMap { s =>
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        JsonUtils.fold(Try(format.parse(s)))(t => Left(t.getMessage), Right(_))
      }
    require(initialPosition != "AT_TIMESTAMP" || timestampEither.isRight, timestampEither.left.getOrElse(""))

    val timestamp = timestampEither.right.toOption

    val endpoint = customEndpoint.getOrElse(region match {
      case cn @ "cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _                 => s"https://kinesis.$region.amazonaws.com"
    })

    val dynamodbEndpoint = dynamodbCustomEndpoint.getOrElse(region match {
      case cn @ "cn-north-1" => s"https://dynamodb.$cn.amazonaws.com.cn"
      case _                 => s"https://dynamodb.$region.amazonaws.com"
    })
  }
  case class BufferConfig(byteLimit: Long, recordLimit: Long, timeLimit: Long)
  case class StreamsConfig(
    inStreamName: String,
    outStreamName: String,
    buffer: BufferConfig
  )
  case class ESClientConfig(
    endpoint: String,
    port: Int,
    username: Option[String],
    password: Option[String],
    maxTimeout: Long,
    maxRetries: Int,
    ssl: Boolean
  )
  case class ESAWSConfig(signing: Boolean, region: String)
  case class ESClusterConfig(
    name: String,
    index: String,
    documentType: String,
    aliasName: Option[String],
    indexDateFormat: Option[String],
    indexDateField: Option[String],
    indexMappingFilePath: Option[String],
    shardsCount: Option[Int],
    replicasCount: Option[Int],
    refreshInterval: Option[String]
  )
  case class ESConfig(
    client: ESClientConfig,
    aws: ESAWSConfig,
    cluster: ESClusterConfig
  )
  case class PostgresConfig(
    server: String,
    port: Int,
    databaseName: String,
    username: String,
    password: String,
    table: String,
    schemas: String,
    shardTableDateField: Option[String],
    shardTableDateFormat: Option[String],
    filterAppId: Option[String]
  )
  case class S3Config(
    bucket: String,
    s3AccessKey: String,
    s3SecretKey: String,
    shardDateField: Option[String],
    shardDateFormat: Option[String]
  )
  case class KinesisConfig(
    kinesisAccessKey: String,
    kinesisSecretKey: String,
    kinesisArnRole: Option[String],
    kinesisStsRegion: Option[String],
    filterAppId: Option[String]
  )
  case class SnowplowMonitoringConfig(
    collectorUri: String,
    collectorPort: Int,
    appId: String,
    method: String
  )
  case class MonitoringConfig(snowplow: SnowplowMonitoringConfig)
  case class StreamLoaderConfig(
    source: String,
    sink: SinkConfig,
    enabled: String,
    aws: AWSConfig,
    queueConfig: QueueConfig,
    streams: StreamsConfig,
    postgres: Option[PostgresConfig],
    elasticsearch: Option[ESConfig],
    s3: Option[S3Config],
    kinesis: Option[KinesisConfig],
    monitoring: Option[MonitoringConfig],
    mappingTable: Option[Map[String, String]],
    deduplicationCache: Option[DeduplicationCache]
  ) {
    val streamType: StreamType = enabled match {
      case "good"       => Good
      case "bad"        => Bad
      case "plain-json" => PlainJson
      case _            => throw new IllegalArgumentException("\"enabled\" must be set to \"good\", \"bad\" or \"plain-json\" ")
    }
  }
  case class DeduplicationCache(
    deduplicationEnabled: Boolean,
    deduplicationField: String,
    sizeLimit: Long,
    timeLimit: Long
  )
}
