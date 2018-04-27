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

package com.snowplowanalytics.elasticsearch.loader

import clients.{BulkSender, BulkSenderTCP}
import com.snowplowanalytics.elasticsearch.loader.utils.SnowplowTracking

/** Main entry point for the Elasticsearch TCP sink */
object ElasticsearchTCPStreamLoaderApp extends App with StreamLoaderApp {
  override lazy val arguments = args

  val config = parseConfig().get
  val esEndpoint = config.elasticsearch.client.endpoint
  val esPort = config.elasticsearch.client.port
  val esClusterName = config.elasticsearch.cluster.name
  val maxConnectionTime = config.elasticsearch.client.maxTimeout
  val tracker = config.monitoring.map(e => SnowplowTracking.initializeTracker(e.snowplow))

  override lazy val bulkSender: BulkSender =
    new BulkSenderTCP(esClusterName, esEndpoint, esPort, tracker, maxConnectionTime)

  run(config)
}
