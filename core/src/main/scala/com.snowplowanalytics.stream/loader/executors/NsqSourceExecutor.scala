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
package com.snowplowanalytics.stream.loader.executors
package executors

// NSQ
import clients.BulkSender
import com.amazonaws.services.kinesis.connectors.elasticsearch.{ElasticsearchEmitter}
import com.snowplowanalytics.client.nsq.{NSQConfig, NSQConsumer, NSQMessage}
import com.snowplowanalytics.client.nsq.callbacks.{NSQErrorCallback, NSQMessageCallback}
import com.snowplowanalytics.client.nsq.exceptions.NSQException
import com.snowplowanalytics.client.nsq.lookup.DefaultNSQLookup
import com.snowplowanalytics.stream.loader.EmitterJsonInput
import model.Config._
import sinks.ISink
import transformers.{BadEventTransformer, PlainJsonTransformer}

//Java
import java.nio.charset.StandardCharsets.UTF_8

// Scala
import scala.collection.mutable.ListBuffer

// Logging
import org.slf4j.LoggerFactory

// This project

/**
 * NSQSource executor
 *
 * @param streamType the type of stream, good, bad or plain-json
 * @param documentIndex the elasticsearch index name
 * @param documentType the elasticsearch index type
 * @param config ESLoader Configuration
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param elasticsearchSender function for sending to elasticsearch
 */
class NsqSourceExecutor(
  streamType: StreamType,
  documentIndex: String,
  documentType: String,
  config: StreamLoaderConfig,
  goodSink: Option[ISink],
  badSink: ISink,
  elasticsearchSender: BulkSender[EmitterJsonInput]
) extends Runnable {

  lazy val log = LoggerFactory.getLogger(getClass())

  // nsq messages will be buffered in msgBuffer until buffer size become equal to nsqBufferSize
  private val msgBuffer = new ListBuffer[EmitterJsonInput]()
  // ElasticsearchEmitter instance

  private val topicName = config.streams.inStreamName
  //private val channelName = config.queueConfig.channelName

  /**
   * Consumer will be started to wait new message.
   */
  override def run(): Unit = { /*
    val nsqCallback = new NSQMessageCallback {
      val nsqBufferSize = config.streams.buffer.recordLimit

      override def message(msg: NSQMessage): Unit = {
        val msgStr = new String(msg.getMessage(), UTF_8)
        msgBuffer.synchronized {
          val emitterInput = transformer.consumeLine(msgStr)
          msgBuffer += emitterInput
          msg.finished()

          if (msgBuffer.size == nsqBufferSize) {
            val elasticsearchRejects = elasticsearchEmitter.attempEmit(msgBuffer.toList)
            elasticsearchEmitter.fail(elasticsearchRejects)
            msgBuffer.clear()
          }
        }
      }
    }

    val errorCallback = new NSQErrorCallback {
      override def error(e: NSQException): Unit =
        log.error(s"Exception while consuming topic $topicName", e)
    }

    // use NSQLookupd
    val lookup = new DefaultNSQLookup
    lookup.addLookupAddress(config.nsq.host, config.nsq.lookupPort)
    val consumer = new NSQConsumer(lookup,
                                   topicName,
                                   channelName,
                                   nsqCallback,
                                   new NSQConfig(),
                                   errorCallback)
    consumer.start() */ }
}
