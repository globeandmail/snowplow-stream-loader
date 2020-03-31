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
package loader

import clients.{BulkSender, BulkSenderTSDB}
import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}
import executors.KinesisSourceExecutor
import model.Config.Kinesis
import pipelines.KinesisPipeline
import java.io.File
import scala.concurrent.duration._

import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.io.Source

// Scalaz
import scalaz._
import Scalaz._

/** Main entry point for the tsdb sink */
object TSDBStreamLoaderApp extends App with StreamLoaderApp {
  override lazy val arguments = args

  lazy val bulkSender: BulkSender[EmitterJsonInput] = config.tsdb match {
    case None =>
      throw new RuntimeException("No configuration for tsdb found.")

    case _ =>
      val dir = new File(config.tsdb.get.schemas)
      val schemas: Map[String, String] = dir.exists && dir.isDirectory match {
        case false => throw new RuntimeException("No schema directory found")
        case true =>
          dir.listFiles
            .filter(_.isFile)
            .toList
            .filter(file => file.getName.endsWith(".sql"))
            .map(file =>
              (
                file.getName.replace(".sql", ""),
                Source.fromFile(file.getAbsolutePath).getLines.mkString("\n")
              )
            )
            .toMap
      }

      val deduplicationCacheRepository: Option[Cache[String, Boolean]] =
        config.deduplicationCache match {
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
        case None      => null
      }

      new BulkSenderTSDB(
        config.tsdb.get.server,
        config.tsdb.get.port,
        config.tsdb.get.databaseName,
        config.tsdb.get.username,
        config.tsdb.get.password,
        config.tsdb.get.table,
        config.tsdb.get.shardTableDateField,
        schemas,
        config.tsdb.flatMap(_.filterAppId),
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
              config.tsdb.get.table,
              config.tsdb.get.shardTableDateField,
              config.tsdb.get.shardTableDateFormat,
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

    case _ => "Source must be set to 'stdin', 'kinesis' or 'nsq'".failure
  }

  run()

}
