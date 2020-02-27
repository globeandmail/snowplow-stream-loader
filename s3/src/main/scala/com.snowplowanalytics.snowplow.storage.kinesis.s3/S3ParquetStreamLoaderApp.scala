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
package com.snowplowanalytics.snowplow.storage.kinesis.s3

import com.snowplowanalytics.snowplow.storage.kinesis.s3.clients.BulkSenderS3Parquet
import com.snowplowanalytics.stream.loader.clients.BulkSender
import com.snowplowanalytics.stream.loader.executors.KinesisSourceExecutor
import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}
import com.snowplowanalytics.stream.loader.model.Config.Kinesis
import com.snowplowanalytics.stream.loader.pipelines.KinesisPipeline
import loader.StreamLoaderApp

// Scalaz
import scalaz.Scalaz._

/** Main entry point for the Postgres sink */
object S3ParquetStreamLoaderApp extends StreamLoaderApp {
  override lazy val arguments = args

  lazy val bulkSender: BulkSender[EmitterJsonInput] = config.s3 match {
    case None =>
      throw new RuntimeException("No configuration for S3 found.")

    case _ =>
      new BulkSenderS3Parquet(
        config.s3.get.bucket,
        config.s3.get.s3AccessKey,
        config.s3.get.s3SecretKey,
        config.s3.get.shardDateField,
        config.s3.get.shardDateFormat
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
              config.s3.get.bucket,
              config.s3.get.shardDateField,
              config.s3.get.shardDateFormat,
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
