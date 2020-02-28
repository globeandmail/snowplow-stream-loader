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

// Java
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import com.snowplowanalytics.stream.loader._
import _root_.model.JsonRecord

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.model.Record

// Scala
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

//scalaz
import scalaz.Scalaz._
import scalaz._


/**
 * Class to convert plain JSON to EmitterInputs
 *
 */
class PlainJsonTransformer
    extends ITransformer[ValidatedJsonRecord, EmitterJsonInput]
    with StdinTransformer {

  /**
   * Convert an Amazon Kinesis record to a json string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedRecord for the event
   */
  override def toClass(record: Record): ValidatedJsonRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    (recordString, toJsonRecord(recordString))
  }
  override def fromClass(record: ValidatedJsonRecord): EmitterJsonInput =
    record.map(_.map(r => r))

  /**
   * Parses a json string as a JsonRecord.
   *
   * @param jsonString the JSON string to be parsed
   * @return the parsed JsonRecord
   */
  private def toJsonRecord(jsonString: String): ValidationNel[String, JsonRecord] = {
    parseOpt(jsonString) match {
      case Some(jvalue) =>
        JsonRecord((jvalue ++ ("id" -> UUID.randomUUID().toString)).asInstanceOf[JObject], None).successNel
      case None => "Json parsing error".failureNel
    }
  }

  /**
   * Consume data from stdin/NSQ rather than Kinesis
   *
   * @param line Line from stdin/NSQ
   * @return Line as an EmitterInput
   */
  def consumeLine(line: String): EmitterJsonInput =
    fromClass(line -> toJsonRecord(line))

}
