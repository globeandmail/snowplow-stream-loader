/**
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.stream.loader.transformers.eventTransformers

// Amazon
import java.util.TimeZone

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.model.Record
import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}
import model.JsonRecord
import scalaz.Scalaz._
import scalaz._

// Java
import java.nio.charset.StandardCharsets.UTF_8
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.joda.time.{DateTime, DateTimeZone}

// Scala
import org.json4s.JsonAST.JString

// cats

// Snowplow
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer._

/**
 * Class to convert successfully enriched events to EmitterInputs
 *
 */
class EnrichedEventJsonTransformer(shardDateField: Option[String], shardDateFormat: Option[String])
    extends ITransformer[ValidatedJsonRecord, EmitterJsonInput]
    with StdinTransformer {

  private val dateFormatter: Option[SimpleDateFormat] = shardDateFormat match {
    case Some(format) => new SimpleDateFormat(format).some
    case _            => None
  }

  private val shardingField = shardDateField.getOrElse("derived_tstamp")

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedJsonRecord for the event
   */


  override def toClass(record: Record): ValidatedJsonRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    (recordString, toJsonRecord(recordString))
  }

  override def fromClass(record: ValidatedJsonRecord): EmitterJsonInput =
    record.map(_.map(r => r))

  /**
   * Parses a string as a JsonRecord.
   * The -1 is necessary to prevent trailing empty strings from being discarded
   *
   * @param record the record to be parsed
   * @return the parsed JsonRecord or a list of failures
   */
  private def toJsonRecord(record: String): ValidationNel[String, JsonRecord] =
    jsonifyGoodEvent(
      record
        .replace("\\u0000", "") // arc sends events with null character and postgres doesn't like it.
        .split("\t", -1)) match {
      case Left(h :: t) => NonEmptyList(h, t: _*).failure
      case Left(Nil)    => "Empty list of failures but reported failure, should not happen".failureNel
      case Right((_, json)) =>

        dateFormatter match {
          case Some(dateF) =>
            val shard = json \ shardingField match {
              case JString(timestampString) =>
                dateF
                  .format(
                    DateTime
                      .parse(timestampString)
                      .withZone(DateTimeZone.UTC)
                      .getMillis
                  )
                  .some
              case _ => None
            }
            JsonRecord(json, shard).success
          case None =>
            JsonRecord(json, None).success
        }
    }

  /**
   * Consume data from stdin rather than Kinesis
   *
   * @param line Line from stdin
   * @return Line as an EmitterJsonInput
   */
  def consumeLine(line: String): EmitterJsonInput =
    fromClass(line -> toJsonRecord(line))
}
