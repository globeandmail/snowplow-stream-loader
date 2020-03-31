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

// Java
import java.util.Base64

// Scala
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.sksamuel.elastic4s.mappings.MappingDefinition
import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import com.snowplowanalytics.stream.loader.EmitterJsonInput
import model.Config.{Bad, StreamType}
import org.apache.http.{Header, HttpHost}
import org.apache.http.message.BasicHeader
import org.apache.thrift.TDeserializer
import org.elasticsearch.client.{RestClient, RestClientBuilder}
import org.json4s.JsonAST.{JObject, JString}

import scala.util.{Failure => SFailure, Success => SSuccess}

// elastic4s
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.{HttpClient, NoOpHttpClientConfigCallback}
import com.sksamuel.elastic4s.http.ElasticDsl._

import org.json4s.jackson.JsonMethods._

// Scalaz
import scalaz._
import Scalaz._

// AMZ
import com.amazonaws.auth.AWSCredentialsProvider

// SLF4j
import org.slf4j.LoggerFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

import scala.concurrent.ExecutionContext.Implicits.global

class BulkSenderHTTP(
  region: String,
  awsSigning: Boolean,
  endpoint: String,
  port: Int,
  username: Option[String],
  password: Option[String],
  ssl: Boolean,
  override val maxConnectionWaitTimeMs: Long = 60000L,
  override val maxAttempts: Int = 6,
  indexName: String,
  documentType: String,
  streamType: StreamType,
  shardsCount: Int,
  replicasCount: Int,
  refreshInterval: String,
  indexMappingSource: Option[String],
  credentialsProvider: AWSCredentialsProvider,
  override val tracker: Option[Tracker] = None
) extends BulkSender[EmitterJsonInput] {
  require(maxAttempts > 0)
  require(maxConnectionWaitTimeMs > 0)

  override val log = LoggerFactory.getLogger(getClass)

  private val uri = ElasticsearchClientUri(s"elasticsearch://$endpoint:$port?ssl=$ssl")
  private val httpClientConfigCallback =
    if (awsSigning) new SignedHttpClientConfigCallback(credentialsProvider, region)
    else NoOpHttpClientConfigCallback

  private val formedHost =
    new HttpHost(endpoint, port, if (uri.options.getOrElse("ssl", "false") == "true") "https" else "http")
  private val restClientBuilder = RestClient
    .builder(formedHost)
    .setHttpClientConfigCallback(
      httpClientConfigCallback.asInstanceOf[RestClientBuilder.HttpClientConfigCallback]
    )

  private val thriftDeserializer = new TDeserializer()

  if (username.orNull != null && password.orNull != null) {
    val userpass = BaseEncoding.base64().encode(s"${username.get}:${password.get}".getBytes(Charsets.UTF_8))
    val headers: Array[Header] = Array(new BasicHeader("Authorization", s"Basic $userpass"))
    restClientBuilder.setDefaultHeaders(headers)
  }

  private val client: HttpClient = HttpClient.fromRestClient(restClientBuilder.build())

  // do not close the es client, otherwise it will fail when resharding
  override def close(): Unit = ()

  def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {
    val connectionAttemptStartTime = System.currentTimeMillis()

    val (successes, oldFailures) = records.partition(_._2.isSuccess)
    val successfulRecords = successes.collect {
      case (_, Success(r)) =>
        val index = r.partition match {
          case null      => indexName
          case partition => indexName + partition
        }
        val jsonToSend = utils.JsonUtils.extractField(r.json, "line") match {
          case Some(base64) if streamType == Bad =>
            try {
              val collectorPayload = new CollectorPayload
              thriftDeserializer.deserialize(collectorPayload, Base64.getDecoder.decode(base64))
              r.json
                .merge(JObject("payload" -> JString(collectorPayload.toString)))
            } catch {
              case _: Exception => r.json
            }
          case None => r.json
        }
        utils.JsonUtils.extractEventId(r.json) match {
          case Some(id) => new ElasticsearchObject(index, documentType, id, compact(render(jsonToSend)))
          case None     => new ElasticsearchObject(index, documentType, compact(render(jsonToSend)))
        }
    }

    // blindly creating the index, it will spit an ignored error result if the index would exists/
    indexMappingSource match {
      case Some(source: String) =>
        successfulRecords
          .map(_.getIndex)
          .toSet[String]
          .map(index =>
            client.execute {
              createIndex(index)
                .shards(shardsCount)
                .replicas(replicasCount)
                .refreshInterval(refreshInterval)
                .mappings(
                  MappingDefinition(`type` = documentType, rawSource = Some(source))
                )
            }.await
          )
      case _ =>
    }

    val actions = successfulRecords
      .map(r => indexInto(r.getIndex / r.getType) id r.getId doc r.getSource)

    val newFailures: List[EmitterJsonInput] = if (actions.nonEmpty) {
      futureToTask(client.execute(bulk(actions)))
      // we retry with linear backoff if an exception happened
        .retry(delays, exPredicate(connectionAttemptStartTime))
        .map {
          {
            case Right(bulkResponseResponse) =>
              bulkResponseResponse.result.items
                .zip(records)
                .flatMap {
                  case (bulkResponseItem, record) =>
                    handleResponse(bulkResponseItem.error.map(_.reason), record)
                }
          }
        }
        .attempt
        .unsafePerformSync match {
        case \/-(s) => s.toList
        case -\/(f) =>
          log.error(
            s"Shutting down application as unable to connect to Elasticsearch for over $maxConnectionWaitTimeMs ms",
            f
          )
          // if the request failed more than it should have we force shutdown
          forceShutdown()
          Nil
      }
    } else Nil

    log.info(s"Emitted ${successfulRecords.size - newFailures.size} records to Elasticseacrch")
    if (newFailures.nonEmpty) logHealth()

    val allFailures = oldFailures ++ newFailures

    if (allFailures.nonEmpty) log.warn(s"Returning ${allFailures.size} records as failed")

    allFailures
  }

  /** Logs the cluster health */
  override def logHealth(): Unit =
    client.execute(clusterHealth) onComplete {
      case SSuccess(health) =>
        health match {
          case Left(_) => log.error("no connection")
          case Right(response) =>
            response.result.status match {
              case "green"  => log.info("Cluster health is green")
              case "yellow" => log.warn("Cluster health is yellow")
              case "red"    => log.error("Cluster health is red")
            }
        }
      case SFailure(e) => log.error("Couldn't retrieve cluster health", e)
    }

  /**
   * Handle the response given for a bulk request, by producing a failure if we failed to insert
   * a given item.
   * @param error possible error
   * @param record associated to this item
   * @return a failure if an unforeseen error happened (e.g. not that the document already exists)
   */
  def handleResponse(
    error: Option[String],
    record: EmitterJsonInput
  ): Option[EmitterJsonInput] = {
    error.foreach(e => log.error(s"Record [$record] failed with message $e"))
    error
      .map { e =>
        if (e.contains("DocumentAlreadyExistsException") || e.contains("VersionConflictEngineException"))
          None
        else
          Some(
            record._1.take(maxSizeWhenReportingFailure) ->
              s"Elasticsearch rejected record with message $e".failureNel
          )
      }
      .getOrElse(None)
  }
}
