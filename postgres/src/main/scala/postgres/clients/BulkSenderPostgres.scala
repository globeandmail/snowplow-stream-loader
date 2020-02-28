/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package clients

import com.github.blemale.scaffeine.Cache
import com.snowplowanalytics.stream.loader.EmitterJsonInput
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import model.JsonRecord
import scalaz._
import Scalaz._
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.parse

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class BulkSenderPostgres(
  val host: String,
  val port: Int,
  val database: String,
  val username: String,
  val password: String,
  val parentTable: String,
  val partitionDateField: Option[String],
  val partitionDateFormat: Option[String],
  val schemas: Map[String, String],
  val filterAppId: Option[String],
  deduplicationCacheRepository: Option[Cache[String, Boolean]],
  deduplicationField: String,
  override val tracker: Option[Tracker] = None
) extends BulkSender[EmitterJsonInput]
    with UsingPostgres {

  val maxConnectionWaitTimeMs: Long = 100000
  val maxAttempts: Int              = 2

  case object PostgresFilterTypes {
    val APP_ID = "app_id"
  }
  // do not close the es client, otherwise it will fail when resharding
  def close(): Unit = ()

  def deduplicate(list: List[JsonRecord]): (List[JsonRecord], List[JsonRecord]) =
    deduplicationCacheRepository match {
      case None => (List(), list)
      case Some(cache) =>
        list.partition(
          record =>
            utils.JsonUtils.extractField(record.json, deduplicationField) match {
              case Some(key) =>
                val result = cache.getIfPresent(key).getOrElse(false)
                cache.put(key, true)
                result
              case None => true // if the field doesn't exist, consider non-duplicated
            }
        )
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

  def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {
    val connectionAttemptStartTime         = System.currentTimeMillis()
    val (successes, oldFailures)           = records.partition(_._2.isSuccess)
    val successfulRecordsToCheck           = successes.collect { case (_, Success(record)) => record }
    val (deduplication, successfulRecords) = deduplicate(successfulRecordsToCheck)
    val filterVals                         = filterAppId.map(_.split(",").map(_.trim).toList).getOrElse(Nil)
    val newFailures: List[EmitterJsonInput] = if (successfulRecords.nonEmpty) {
      successfulRecords
        .groupBy(r => r.partition)
        .mapValues { recordsForPartition =>
          {
            if (filterVals.nonEmpty) {
              val filteredRecords = recordsForPartition.filter { rec =>
                val extractedValueOption = extractStringElementFromJson(PostgresFilterTypes.APP_ID, rec.json.asInstanceOf[JObject])
                extractedValueOption.exists(filterVals.contains)
              }
              println("#################filtereddddddddddddddd redordssssssssssssssssss"+filteredRecords)
              filteredRecords
            } else {
              recordsForPartition
            }
          }
        }
        .filter(_._2.nonEmpty)
        .map {
          case (Some(partitionName), recordsForPartition) =>
            futureToTask(Future { write(partitionName, recordsForPartition) })
              .retry(delays, exPredicate(connectionAttemptStartTime))
              .map {
                {
                  case response =>
                    response.zip(records).flatMap {
                      case (responseItem, record) =>
                        handleResponse(responseItem, record)
                    }
                }
              }
              .attempt
              .unsafePerformSync match {
              case \/-(s) => s.toList
              case -\/(f) =>
                log.error(
                  s"Shutting down application as unable to connect to datastore for over $maxConnectionWaitTimeMs ms",
                  f
                )
                // if the request failed more than it should have we force shutdown
                forceShutdown()
                Nil
            }
        }
        .flatten
        .toList
    } else Nil

    log.info(s"Emitted ${successfulRecords.size - newFailures.size} records, ${deduplication.size} duplicated ignored")
    if (newFailures.nonEmpty) logHealth()
    val allFailures = oldFailures ++ newFailures
    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")
    allFailures
  }

  /** Logs the  health */
  def logHealth(): Unit = {}

  // https://www.postgresql.org/docs/current/errcodes-appendix.html
  def handleResponse(
    response: String,
    record: EmitterJsonInput
  ): Option[EmitterJsonInput] =
    if (response != "00000") { //failure
      Some(
        record._1.take(maxSizeWhenReportingFailure) ->
          s"Postgres rejected record with message $response".failureNel
      )
    } else {
      None
    }
}
