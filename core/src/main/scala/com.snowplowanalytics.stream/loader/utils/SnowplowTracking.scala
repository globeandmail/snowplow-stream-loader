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
package utils

// json4s
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.scalatracker.emitters.AsyncBatchEmitter
import com.snowplowanalytics.stream.loader.model.Config.SnowplowMonitoringConfig
import org.json4s.JsonDSL._
import org.json4s._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Functionality for sending Snowplow events for monitoring purposes
 */
object SnowplowTracking {

  private val HeartbeatInterval = 300000L // TODO: add it to the config
  private val ThirtyTwo = 32
  /**
   * Configure a Tracker based on the configuration HOCON
   *
   * @param config The "monitoring" section of the HOCON
   * @return a new tracker instance
   */
  def initializeTracker(config: SnowplowMonitoringConfig): Tracker = {
    val endpoint = config.collectorUri
    val port     = Some(config.collectorPort)
    val appName  = config.appId
    // Not yet used
    val method  = config.method // TODO: use it somehow!!
    val emitter = AsyncBatchEmitter.createAndStart(endpoint, port, bufferSize = ThirtyTwo) // TODO: add buffer to the config, buffer is with post though
    new Tracker(List(emitter), appName, appName)
  }

  /**
   * If a tracker has been configured, send a sink_write_failed event
   *
   * @param tracker a Tracker instance
   * @param lastRetryPeriod the backoff period between attempts
   * @param failureCount the number of consecutive failed writes
   * @param initialFailureTime Time of the first consecutive failed write
   * @param message What went wrong
   */
  def sendFailureEvent(
    tracker: Tracker,
    lastRetryPeriod: Long,
    failureCount: Long,
    initialFailureTime: Long,
    storageType: String,
    message: String
  ): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey.fromUri("iglu:com.snowplowanalytics.monitoring.kinesis/storage_write_failed/jsonschema/1-0-0").get,
        ("storage"              -> storageType) ~
          ("failureCount"       -> failureCount) ~
          ("initialFailureTime" -> initialFailureTime) ~
          ("lastRetryPeriod"    -> lastRetryPeriod) ~
          ("message"            -> message)
      )
    )

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker a Tracker instance
   */
  def initializeSnowplowTracking(tracker: Tracker): Unit = {
    trackApplicationInitialization(tracker)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit =
        trackApplicationShutdown(tracker)
    })

    val heartbeatThread = new Thread {
      override def run(): Unit =
        while (true) {
          trackApplicationHeartbeat(tracker, HeartbeatInterval)
          Thread.sleep(HeartbeatInterval)
        }
    }

    heartbeatThread.start()
  }

  /**
   * Send an application_initialized unstructured event
   *
   * @param tracker a Tracker instance
   */
  private def trackApplicationInitialization(tracker: Tracker): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey.fromUri("iglu:com.snowplowanalytics.monitoring.kinesis/app_initialized/jsonschema/1-0-0").get,
        JObject(Nil) // TODO: fill applicationName
      )
    )

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker a Tracker instance
   */
  def trackApplicationShutdown(tracker: Tracker): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey.fromUri("iglu:com.snowplowanalytics.monitoring.kinesis/app_shutdown/jsonschema/1-0-0").get,
        JObject(Nil)
      )
    )

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat(tracker: Tracker, heartbeatInterval: Long): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData(
        SchemaKey.fromUri("iglu:com.snowplowanalytics.monitoring.kinesis/app_heartbeat/jsonschema/1-0-0").get,
        "interval" -> heartbeatInterval
      )
    )
}
