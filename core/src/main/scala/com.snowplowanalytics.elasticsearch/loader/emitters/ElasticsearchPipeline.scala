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

package com.snowplowanalytics.elasticsearch.loader.emitters

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{AllPassFilter, BasicMemoryBuffer}
import com.amazonaws.services.kinesis.connectors.interfaces.{IEmitter, IKinesisConnectorPipeline}
import com.snowplowanalytics.elasticsearch.loader.clients.BulkSender
import com.snowplowanalytics.elasticsearch.loader.model.{Bad, Good, PlainJson, StreamType}
import com.snowplowanalytics.elasticsearch.loader.sinks.ISink
import com.snowplowanalytics.elasticsearch.loader.{EmitterInput, ValidatedRecord}
import com.snowplowanalytics.elasticsearch.loader.transformers.{BadEventTransformer, PlainJsonTransformer, SnowplowElasticsearchTransformer}
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project

// Tracker

// This project

/**
 * KinesisElasticsearchPipeline class sets up the Emitter/Buffer/Transformer/Filter
 *
 * @param streamType the type of stream, good, bad or plain-json
 * @param documentIndex the elasticsearch index name
 * @param documentType the elasticsearch index type
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param elasticsearchSender The ES Client to use
 * @param tracker a Tracker instance
 */
class KinesisElasticsearchPipeline(
                                    streamType: StreamType,
                                    documentIndex: String,
                                    documentType: String,
                                    goodSink: Option[ISink],
                                    badSink: ISink,
                                    elasticsearchSender: BulkSender,
                                    tracker: Option[Tracker] = None
) extends IKinesisConnectorPipeline[ValidatedRecord, EmitterInput] {

  override def getEmitter(configuration: KinesisConnectorConfiguration): IEmitter[EmitterInput] =
    new KinesisElasticsearchEmitter(configuration, goodSink, badSink, elasticsearchSender, tracker)

  override def getBuffer(configuration: KinesisConnectorConfiguration) = new BasicMemoryBuffer[ValidatedRecord](configuration)

  override def getTransformer(c: KinesisConnectorConfiguration) = streamType match {
    case Good => new SnowplowElasticsearchTransformer(documentIndex, documentType)
    case Bad => new BadEventTransformer(documentIndex, documentType)
    case PlainJson => new PlainJsonTransformer(documentIndex, documentType)
  }

  override def getFilter(c: KinesisConnectorConfiguration) = new AllPassFilter[ValidatedRecord]()
}
