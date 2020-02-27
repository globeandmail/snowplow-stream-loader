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
package transformers

// Java
import java.nio.charset.StandardCharsets.UTF_8

import com.snowplowanalytics.stream.loader.{EmitterJsonInput, ValidatedJsonRecord}
import com.snowplowanalytics.stream.loader.model.JsonRecord
import org.json4s.JsonAST.JObject

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.model.Record

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Scalaz
import scalaz.Scalaz._

// TODO consider giving BadEventTransformer its own types

/**
 * Class to convert bad events to ElasticsearchObjects
 *
 * @param documentIndexOrPrefix elasticsearch index name
 */
class BadEventTransformer(
  documentIndexOrPrefix: String,
  documentIndexSuffixField: Option[String],
  documentIndexSuffixFormat: Option[String]
) extends ITransformer[ValidatedJsonRecord, EmitterJsonInput]
    with StdinTransformer {

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of a bad row string
   * @return JsonRecord containing JSON string for the event and no event_id
   */
  override def toClass(record: Record): ValidatedJsonRecord = {
    val recordString = new String(record.getData.array, UTF_8)
    (recordString, JsonRecord(parse(recordString).asInstanceOf[JObject], null).success)
  }

  /**
   * Convert a buffered bad event JSON to an ElasticsearchObject
   *
   * @param record JsonRecord containing a bad event JSON
   * @return An ElasticsearchObject
   */
  override def fromClass(record: ValidatedJsonRecord): EmitterJsonInput =
    (record._1, record._2.map(j => j))

  /**
   * Consume data from stdin rather than Kinesis
   *
   * @param line Line from stdin
   * @return Line as an EmitterJsonInput
   */
  def consumeLine(line: String): EmitterJsonInput =
    fromClass(line -> JsonRecord(parse(line).asInstanceOf[JObject], null).success)
}
