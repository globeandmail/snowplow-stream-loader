
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
package com.snowplowanalytics.stream.loader.sinks

// Java
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import org.json4s.DefaultFormats
import org.json4s.JsonAST.JObject

import scala.util.Try

// Scala
import scala.util.Random
import scala.collection.JavaConverters._

// Amazon
import com.amazonaws.services.kinesis.model._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder

// Concurrent libraries
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

// SLF4j
import org.slf4j.LoggerFactory

/**
 * Kinesis Sink
 *
 * @param provider AWSCredentialsProvider
 * @param endpoint Kinesis stream endpoint
 * @param region   Kinesis region
 * @param name     Kinesis stream name
 */
class KinesisSink(
  provider: AWSCredentialsProvider,
  endpoint: String,
  region: String,
  name: String,
  filterAppId: Option[String]
) extends ISink {

  private lazy val log = LoggerFactory.getLogger(getClass)
  implicit val formats = DefaultFormats

  case object KinesisFilterTypes {
    val APP_ID = "app_id"
  }

  // Explicitly create a client so we can configure the end point
  val client = AmazonKinesisClientBuilder
    .standard()
    .withCredentials(provider)
    .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
    .build()

  require(streamExists(name), s"Stream $name doesn't exist or is neither active nor updating (deleted or creating)")

  /**
   * Checks if a stream exists.
   *
   * @param name Name of the stream to look for
   * @return Whether the stream both exists and is active
   */
  def streamExists(name: String): Boolean =
    try {
      val describeStreamResult = client.describeStream(name)
      val status               = describeStreamResult.getStreamDescription.getStreamStatus
      status == "ACTIVE" || status == "UPDATING"
    } catch {
      case rnfe: ResourceNotFoundException => false
    }

  private def put(name: String, data: ByteBuffer, key: String): Future[PutRecordResult] = Future {
    val putRecordRequest = {
      val p = new PutRecordRequest()
      p.setStreamName(name)
      p.setData(data)
      p.setPartitionKey(key)
      p
    }
    client.putRecord(putRecordRequest)
  }

  /**
   * Write a record to the Kinesis stream
   *
   * @param output The string record to write
   * @param key    A hash of the key determines to which shard the
   *               record is assigned. Defaults to a random string.
   * @param good   Unused parameter which exists to extend ISink
   */
  def store(output: String, key: Option[String], good: Boolean) =
    put(name, ByteBuffer.wrap(output.getBytes(UTF_8)), key.getOrElse(Random.nextInt.toString)) onComplete {
      case Success(result) => {
        //log.info(s"Writing successful - ShardId: ${result.getShardId} SequenceNumber: ${result.getSequenceNumber}")
      }
      case Failure(f) => {
        log.error("Writing to Kinesis failed: ", f)
      }
    }

  /**
   * Type Safe method for extracting key from JsonAST
   *
   * @param key  The key to extract
   * @param json The JsonAST Object to extract the key From
   * @return
   */
  def extractStringElementFromJson(key: String, json: JObject): Option[String] =
    //If key exists
    if (json.values.keySet.contains(key)) {
      Try((json \\ key).extract[String]).toOption
    } else {
      None
    }

  def store(batch: List[(String, JObject)], key: Option[String], good: Boolean): List[String] =
    try {
      val partitionKey = key.getOrElse(Random.nextInt.toString)
      val filteredBatch: List[String] = filterAppId match {
        //If filters are present then filter them from stream
        case Some(filterValue) if filterValue.nonEmpty =>
          batch
            .map { rec =>
              val extractedValueOption = extractStringElementFromJson(KinesisFilterTypes.APP_ID, rec._2)
              val filterVals           = filterValue.split(",").map(_.trim)

              extractedValueOption match {
                //Return tsv string if we were able to successfully extract the key and find in filterVals
                case Some(v) if filterVals.contains(v) => rec._1
                //Return empty String
                case _ => ""
              }

            }
            .filter(_.nonEmpty)

        //Let everything pass
        case _ => batch.map(_._1)

      }

      if (filteredBatch.nonEmpty) {
        val putRecordsRequest = {
          val prr = new PutRecordsRequest()
          prr.setStreamName(name)
          val putRecordsRequestEntryList = filteredBatch.map { b =>
            val prre = new PutRecordsRequestEntry()
            prre.setPartitionKey(partitionKey)
            prre.setData(ByteBuffer.wrap(b.getBytes(UTF_8)))
            prre
          }
          prr.setRecords(putRecordsRequestEntryList.asJava)
          prr
        }
        val results = client.putRecords(putRecordsRequest).getRecords.asScala.toList
        val failurePairs = filteredBatch zip results filter {
          _._2.getErrorMessage != null

        }
        log.info(s"Successfully wrote ${filteredBatch.size - failurePairs.size} out of ${filteredBatch.size} records")
        if (failurePairs.nonEmpty) {
          failurePairs.foreach(
            f =>
              log.error(s"Record failed with error code [${f._2.getErrorCode}] and message [${f._2.getErrorMessage}]")
          )
          failurePairs.map(_._1)
        } else List()
      } else {
        List()
      }

    } catch {
      case e: Exception =>
        log.error("error when writing: ", e)
        throw e
    }
}
