/*
 * © Copyright 2020 The Globe and Mail
 */
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
package com.snowplowanalytics.stream.loader.emitter

import com.snowplowanalytics.stream.loader.clients.BulkSender
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter
import com.snowplowanalytics.stream.loader.EmitterJsonInput
import com.snowplowanalytics.stream.loader.model.BadRow

import scala.collection.mutable.{Buffer => SMBuffer}

// Java
import java.io.IOException
import java.util.List

// Scalaz
import scalaz._

// Scala
import com.snowplowanalytics.stream.loader.sinks.ISink

import scala.collection.JavaConverters._
import scala.collection.immutable.{List => SList}
import scala.collection.mutable.ListBuffer

/**
 * ElasticsearchEmitter class for any sort of BulkSender Extension
 *
 * @param bulkSender        The bulkSender Client to use
 * @param goodSink          the configured GoodSink
 * @param badSink           the configured BadSink
 * @param bufferRecordLimit record limit for buffer
 * @param bufferByteLimit   byte limit for buffer
 */
class Emitter(
               bulkSender: BulkSender[EmitterJsonInput],
               goodSink: Option[ISink],
               badSink: ISink,
               bufferRecordLimit: Long,
               bufferByteLimit: Long
             ) extends IEmitter[EmitterJsonInput] {

  @throws[IOException]
  def emit(buffer: UnmodifiableBuffer[EmitterJsonInput]): List[EmitterJsonInput] =
    attemptEmit(buffer.getRecords)

  /**
   * Emits good records to stdout or Elasticsearch.
   * All records which Elasticsearch rejects and all records which failed transformation
   * get sent to to stderr or Kinesis.
   *
   * @param records list containing EmitterInputs
   * @return list of inputs which failed transformation or which Elasticsearch rejected
   */
  @throws[IOException]
  def attemptEmit(records: List[EmitterJsonInput]): List[EmitterJsonInput] =
    if (records.asScala.isEmpty) {
      null
    } else {
      val (validRecords: SList[EmitterJsonInput], invalidRecords: SList[EmitterJsonInput]) =
        records.asScala.toList.partition(_._2.isSuccess)
      // Send all valid records to stdout / Sink and return those rejected by it
      val rejects = goodSink match {
        case Some(s) =>
          validRecords.foreach {
            case (_, record) => record.map(r => s.store(r.json.toString, None, true))
          }
          Nil
        case None if validRecords.isEmpty => Nil
        case _ => emit(validRecords)
      }
      (invalidRecords ++ rejects).asJava

    }

  /**
   * Emits good records to Elasticsearch and bad records to Kinesis.
   * All valid records in the buffer get sent to Elasticsearch in a bulk request.
   * All invalid requests and all requests which failed transformation get sent to Kinesis.
   *
   * @param records List of records to send to Elasticsearch
   * @return List of inputs which Elasticsearch rejected
   */
  def emit(records: SList[EmitterJsonInput]): SMBuffer[EmitterJsonInput] = {
    val failures: SList[SList[EmitterJsonInput]] = for {
      recordSlice <- splitBuffer(records, bufferByteLimit, bufferRecordLimit)
    } yield bulkSender.send(recordSlice)
    failures.flatten.toBuffer
  }

  /**
   * Splits the buffer into emittable chunks based on the
   * buffer settings defined in the config
   *
   * @param records The records to split
   * @returns a list of buffers
   */
  def splitBuffer(
                   records: SList[EmitterJsonInput],
                   byteLimit: Long,
                   recordLimit: Long
                 ): SList[SList[EmitterJsonInput]] = {
    // partition the records in
    val remaining: ListBuffer[EmitterJsonInput] = records.to[ListBuffer]
    val buffers: ListBuffer[SList[EmitterJsonInput]] = new ListBuffer
    val curBuffer: ListBuffer[EmitterJsonInput] = new ListBuffer
    var runningByteCount: Long = 0L

    while (remaining.nonEmpty) {
      val record = remaining.remove(0)

      val byteCount: Long = record match {
        case (_, Success(obj)) => obj.toString.getBytes("UTF-8").length.toLong
        case (_, Failure(_)) => 0L // This record will be ignored in the sender
      }

      if ((curBuffer.length + 1) > recordLimit || (runningByteCount + byteCount) > byteLimit) {
        // add this buffer to the output and start a new one with this record
        // (if the first record is larger than the byte limit the buffer will be empty)
        if (curBuffer.nonEmpty) {
          buffers += curBuffer.toList
          curBuffer.clear()
        }
        curBuffer += record
        runningByteCount = byteCount
      } else {
        curBuffer += record
        runningByteCount += byteCount
      }
    }

    // add any remaining items to the final buffer
    if (curBuffer.nonEmpty) buffers += curBuffer.toList

    buffers.toList
  }

  /**
   * Closes the Elasticsearch client when the KinesisConnectorRecordProcessor is shut down
   */
  def shutdown(): Unit = bulkSender.close

  /**
   * Handles records rejected by the ElasticsearchTransformer or by Elasticsearch
   *
   * @param records List of failed records
   */
  def fail(records: List[EmitterJsonInput]): Unit =
    records.asScala.foreach {
      _ match {
        case (r: String, Failure(fs)) =>
          val output = BadRow(r, fs).toCompactJson
          badSink.store(output, None, false)
        case (_, Success(_)) => ()
      }
    }

}
