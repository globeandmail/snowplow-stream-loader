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
package postgres

import clients.{BulkSender, BulkSenderPostgres}
import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}
import model.Config.{Kafka, Kinesis}
import pipelines.KinesisPipeline
import java.io.File

import scala.concurrent.duration._
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.snowplowanalytics.stream.loader.executors.{KafkaSourceExecutor, KinesisSourceExecutor}
import loader.StreamLoaderApp

import scala.io.Source

// Scalaz
import scalaz._
import Scalaz._

/** Main entry point for the Postgres sink */
object PostgresStreamLoaderApp extends StreamLoaderApp {
  override lazy val arguments = args

  lazy val bulkSender: BulkSender[EmitterJsonInput] = config.postgres match {
    case None =>
      throw new RuntimeException("No configuration for Postgres found.")

    case _ =>
      val dir = new File(config.postgres.get.schemas)
      val schemas: Map[String, String] = dir.exists && dir.isDirectory match {
        case false => throw new RuntimeException("No schema directory found")
        case true =>
          dir.listFiles
            .filter(_.isFile)
            .toList
            .filter { file =>
              file.getName.endsWith(".sql")
            }
            .map(
              file => (file.getName.replace(".sql", ""), Source.fromFile(file.getAbsolutePath).getLines.mkString("\n"))
            )
            .toMap
      }

      val deduplicationCacheRepository: Option[Cache[String, Boolean]] = config.deduplicationCache match {
        case Some(dcs) =>
          if (!dcs.deduplicationEnabled) {
            None
          } else {
            Scaffeine()
              .expireAfterWrite(dcs.timeLimit.seconds)
              .maximumSize(dcs.sizeLimit)
              .build[String, Boolean]()
              .some
          }
        case None => None
      }

      val deduplicationField = config.deduplicationCache match {
        case Some(dcs) => dcs.deduplicationField
        case None => null
      }

      new BulkSenderPostgres(
        config.postgres.get.server,
        config.postgres.get.port,
        config.postgres.get.databaseName,
        config.postgres.get.username,
        config.postgres.get.password,
        config.postgres.get.table,
        config.postgres.get.shardTableDateField,
        config.postgres.get.shardTableDateFormat,
        schemas,
        config.postgres.flatMap(_.filterAppId),
        deduplicationCacheRepository,
        deduplicationField
      )
  }

  lazy val executor = config.source match {
    // Read records from Kinesis
    case "kinesis" =>
      config.queueConfig match {
        case queueConfig: Kinesis =>
          new KinesisSourceExecutor[ValidatedJsonRecord, EmitterJsonInput](
            config,
            queueConfig.initialPosition,
            queueConfig.timestamp,
            queueConfig.initialRCU,
            queueConfig.initialWCU,
            new KinesisPipeline(
              config.streamType,
              config.postgres.get.table,
              config.postgres.get.shardTableDateField,
              config.postgres.get.shardTableDateFormat,
              config.mappingTable,
              goodSink,
              badSink,
              config.streams.buffer.recordLimit,
              config.streams.buffer.byteLimit,
              bulkSender,
              tracker
            )
          ).success
        case _ => "you cannot select kinesis and then have others".failure
      }
    case "kafka" =>
      config.queueConfig match {
        case queueConfig: Kafka =>
          new KafkaSourceExecutor(config.streamType,
            goodSink,
            badSink,
            bulkSender,
            queueConfig,
            config.postgres.get.shardTableDateField,
            config.postgres.get.shardTableDateFormat,
            config).success

        case _ => "Source must be set to 'stdin', 'kinesis' or 'nsq'".failure
      }

  }
  run()
}