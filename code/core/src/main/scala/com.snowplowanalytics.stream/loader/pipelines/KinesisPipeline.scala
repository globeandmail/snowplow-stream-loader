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
package pipelines

import clients.BulkSender
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{AllPassFilter, BasicMemoryBuffer}
import com.amazonaws.services.kinesis.connectors.interfaces.{IEmitter, IKinesisConnectorPipeline}
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.stream.loader.transformers.JsonTransformer
import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}
import emitter.Emitter
import model.Config.{Bad, Good, PlainJson, StreamType}
import sinks.ISink
import transformers.{BadEventTransformer, PlainJsonTransformer}

/**
 * KinesisPipeline class sets up the ElasticsearchEmitter/Buffer/Transformer/Filter
 *
 * @param streamType                the type of stream, good, bad or plain-json
 * @param documentIndexOrPrefix     the elasticsearch index name
 * @param documentIndexSuffixField  the elasticsearch index name
 * @param documentIndexSuffixFormat the elasticsearch index name
 * @param goodSink                  the configured GoodSink
 * @param badSink                   the configured BadSink
 * @param bulkSender                The Bulksender Client to use
 * @param tracker                   a Tracker instance
 */
class KinesisPipeline(
  streamType: StreamType,
  documentIndexOrPrefix: String,
  documentIndexSuffixField: Option[String],
  documentIndexSuffixFormat: Option[String],
  mappingTable: Option[Map[String, String]],
  goodSink: Option[ISink],
  badSink: ISink,
  bufferRecordLimit: Long,
  bufferByteLimit: Long,
  bulkSender: BulkSender[EmitterJsonInput],
  tracker: Option[Tracker] = None
) extends IKinesisConnectorPipeline[ValidatedJsonRecord, EmitterJsonInput] {

  override def getEmitter(
    configuration: KinesisConnectorConfiguration
  ): IEmitter[EmitterJsonInput] =
    new Emitter(bulkSender, goodSink, badSink, bufferRecordLimit, bufferByteLimit)

  override def getBuffer(configuration: KinesisConnectorConfiguration) =
    new BasicMemoryBuffer[ValidatedJsonRecord](configuration)

  override def getTransformer(c: KinesisConnectorConfiguration) = streamType match {
    case Good =>
      new JsonTransformer(
        documentIndexOrPrefix,
        documentIndexSuffixField,
        documentIndexSuffixFormat,
        mappingTable
      )
    case Bad =>
      new BadEventTransformer(
        documentIndexOrPrefix,
        documentIndexSuffixField,
        documentIndexSuffixFormat
      )
    case PlainJson =>
      new PlainJsonTransformer(
        documentIndexOrPrefix,
        documentIndexSuffixField,
        documentIndexSuffixFormat,
        mappingTable
      )
  }

  override def getFilter(c: KinesisConnectorConfiguration) =
    new AllPassFilter[ValidatedJsonRecord]()
}
