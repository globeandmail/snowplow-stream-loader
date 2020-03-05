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

import clients.{BulkSender, BulkSenderHTTP}
import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}
import executors.KinesisSourceExecutor
import model.Config.Kinesis
import pipelines.KinesisPipeline
import utils.CredentialsLookup

import scala.io.Source

// Scalaz
import scalaz._
import Scalaz._

/** Main entry point for the Elasticsearch HTTP sink */
object ElasticsearchHTTPStreamLoaderApp extends StreamLoaderApp {

  val bulkSender: BulkSender[EmitterJsonInput] = config.elasticsearch match {
    case None =>
      throw new RuntimeException("No configuration for Elasticsearch found.")
    case _ =>
      val schema = config.elasticsearch.get.cluster.indexMappingFilePath match {
        case Some(path) => Some(Source.fromFile(path).getLines.mkString("\n"))
        case None       => None
      }
      new BulkSenderHTTP(
        config.elasticsearch.get.aws.region,
        config.elasticsearch.get.aws.signing,
        config.elasticsearch.get.client.endpoint,
        config.elasticsearch.get.client.port,
        config.elasticsearch.get.client.username,
        config.elasticsearch.get.client.password,
        config.elasticsearch.get.client.ssl,
        config.elasticsearch.get.client.maxTimeout,
        config.elasticsearch.get.client.maxRetries,
        config.elasticsearch.get.cluster.index,
        config.elasticsearch.get.cluster.documentType,
        config.streamType,
        config.elasticsearch.get.cluster.shardsCount.getOrElse(10),
        config.elasticsearch.get.cluster.replicasCount.getOrElse(2),
        config.elasticsearch.get.cluster.refreshInterval.getOrElse("1m"),
        schema,
        CredentialsLookup
          .getCredentialsProvider(config.aws.accessKey, config.aws.secretKey, config.aws.arnRole, config.aws.stsRegion),
        tracker = tracker
      )
  }

  val executor = config.source match {
    // Read records from Kinesis
    case "kinesis" =>
      config.queueConfig match {
        case sourceConfig: Kinesis =>
          new KinesisSourceExecutor[ValidatedJsonRecord, EmitterJsonInput](
            config,
            sourceConfig.initialPosition,
            sourceConfig.timestamp,
            sourceConfig.initialRCU,
            sourceConfig.initialWCU,
            new KinesisPipeline(
              config.streamType,
              config.elasticsearch.get.cluster.index,
              config.elasticsearch.get.cluster.indexDateField,
              config.elasticsearch.get.cluster.indexDateFormat,
              config.mappingTable,
              goodSink,
              badSink,
              config.streams.buffer.recordLimit,
              config.streams.buffer.byteLimit,
              bulkSender,
              tracker
            )
          ).success
        case _ => "Kinesis is selected as the source, but no configuration defined".failure
      }
    case _ => "Source must be set to 'stdin', 'kinesis' or 'nsq'".failure
    /*// Read records from NSQ
    case "nsq" =>
      new NsqSourceExecutor(streamType,
        documentIndex,
        documentType,
        conf,
        goodSink,
        badSink,
        bulkSender
      ).success

    // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
    // TODO reduce code duplication
    case "stdin" => new Runnable {
      val transformer = streamType match {
        case Good => new JsonTransformer(documentIndex,"","", documentType)
        case Bad => new BadEventTransformer(documentIndex, documentType)
        case PlainJson => new PlainJsonTransformer(documentIndex, documentType)
      }

      def run = for (ln <- scala.io.Source.stdin.getLines) {
        val emitterInput = transformer.consumeLine(ln)
        emitterInput._2.bimap(
          f => badSink.store(BadRow(emitterInput._1, f).toCompactJson, None, false),
          s => goodSink match {
            case Some(gs) => gs.store(s.getSource, None, true)
            case None => bulkSender.send(List(ln -> s.success))
          }
        )
      }
    }.success*/

  }

  run()
}
