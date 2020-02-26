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

import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.stream.loader.EmitterJsonInput
import scalaz._
import Scalaz._
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class BulkSenderS3Parquet(
  val bucket: String,
  val accessKey: String,
  val secretKey: String,
  val partitionDateField: Option[String],
  val partitionDateFormat: Option[String],
  override val tracker: Option[Tracker] = None
) extends BulkSender[EmitterJsonInput]
    with UsingS3Parquet {

  val maxConnectionWaitTimeMs: Long = 10000
  val maxAttempts: Int              = 2
  val LOG                           = LoggerFactory.getLogger(getClass)

  // do not close the es client, otherwise it will fail when resharding
  def close(): Unit = ()

  def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {
    val connectionAttemptStartTime = System.currentTimeMillis()
    val (successes, oldFailures)   = records.partition(_._2.isSuccess)
    val successfulRecords          = successes.collect { case (_, Success(record)) => record }
    val newFailures: List[EmitterJsonInput] = if (successfulRecords.nonEmpty) {
      successfulRecords
        .groupBy(r => r.partition)
        .map({
          case (partitionName, recordsForPartition) =>
            futureToTask(Future { write(partitionName, cleanUpRecords(recordsForPartition)) })
              .retry(delays, exPredicate(connectionAttemptStartTime))
              .map {
                { response =>
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
        })
        .flatten
        .toList
    } else Nil

    log.info(s"Emitted ${successfulRecords.size - newFailures.size} records")
    if (newFailures.nonEmpty) logHealth()
    val allFailures = oldFailures ++ newFailures
    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")
    allFailures
  }

  /** Logs the  health */
  def logHealth(): Unit = {}

  def handleResponse(
    response: Int,
    record: EmitterJsonInput
  ): Option[EmitterJsonInput] =
    if (response < 0) { //failure
      Some(
        record._1.take(maxSizeWhenReportingFailure) ->
          s"S3 rejected record with message $response".failureNel
      )
    } else {
      None
    }

}
