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
package com.snowplowanalytics.stream.loader.executors

// Logging
import java.util.Properties

import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline
import model.Config.{Kinesis, StreamLoaderConfig}
import utils.CredentialsLookup
import org.slf4j.LoggerFactory

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  KinesisConnectorExecutorBase,
  KinesisConnectorRecordProcessorFactory
}

// AWS Client Library
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory

// Java
import java.util.Date

// Tracker

// This project

/**
 * Boilerplate class for Kinesis Connector
 *
 * @param streamLoaderConfig the KCL configuration
 * @param initialPosition initial position for kinesis stream
 * @param initialTimestamp timestamp for "AT_TIMESTAMP" initial position

 */
class KinesisSourceExecutor[A, B](
  streamLoaderConfig: StreamLoaderConfig,
  initialPosition: String,
  initialTimestamp: Option[Date],
  leaseTableInitialReadCapacity: Option[Int],
  leaseTableInitialWriteCapacity: Option[Int],
  kinesisConnectorPipeline: IKinesisConnectorPipeline[A, B]
) extends KinesisConnectorExecutorBase[A, B] {

  val LOG                     = LoggerFactory.getLogger(getClass)
  val DEFAULT_LEASE_TABLE_RCU = 1
  val DEFAULT_LEASE_TABLE_WCU = 5

  /**
   * Builds a KinesisConnectorConfiguration
   *
   * @param config the configuration HOCON
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(config: StreamLoaderConfig): KinesisConnectorConfiguration = {
    val props = new Properties
    config.queueConfig match {
      case queueConfig: Kinesis =>
        props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, queueConfig.endpoint)
        props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, queueConfig.appName.trim)
        props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM, queueConfig.initialPosition)
        props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, queueConfig.maxRecords.toString)
        props.setProperty(KinesisConnectorConfiguration.PROP_DYNAMODB_ENDPOINT, queueConfig.dynamodbEndpoint)
        // So that the region of the DynamoDB table is correct
        props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, queueConfig.region)
      case _ => throw new RuntimeException("No Kinesis configuration for the executor")

    }
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, config.streams.inStreamName)

    if (config.elasticsearch.isDefined) {
      props.setProperty(
        KinesisConnectorConfiguration.PROP_ELASTICSEARCH_ENDPOINT,
        config.elasticsearch.get.client.endpoint
      )
      props.setProperty(
        KinesisConnectorConfiguration.PROP_ELASTICSEARCH_CLUSTER_NAME,
        config.elasticsearch.get.cluster.name
      )
      props.setProperty(
        KinesisConnectorConfiguration.PROP_ELASTICSEARCH_PORT,
        config.elasticsearch.get.client.port.toString
      )
    }

    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
      config.streams.buffer.byteLimit.toString
    )
    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT,
      config.streams.buffer.recordLimit.toString
    )
    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
      config.streams.buffer.timeLimit.toString
    )

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "elasticsearch")
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    new KinesisConnectorConfiguration(
      props,
      CredentialsLookup
        .getCredentialsProvider(config.aws.accessKey, config.aws.secretKey, config.aws.arnRole, config.aws.stsRegion)
    )
  }

  def getKCLConfig(
    initialPosition: String,
    timestamp: Option[Date],
    kcc: KinesisConnectorConfiguration
  ): KinesisClientLibConfiguration = {
    val cfg = new KinesisClientLibConfiguration(
      kcc.APP_NAME,
      kcc.KINESIS_INPUT_STREAM,
      kcc.AWS_CREDENTIALS_PROVIDER,
      kcc.WORKER_ID
    ).withKinesisEndpoint(kcc.KINESIS_ENDPOINT)
      .withFailoverTimeMillis(kcc.FAILOVER_TIME)
      .withMaxRecords(kcc.MAX_RECORDS)
      .withIdleTimeBetweenReadsInMillis(kcc.IDLE_TIME_BETWEEN_READS)
      .withCallProcessRecordsEvenForEmptyRecordList(
        KinesisConnectorConfiguration.DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST
      )
      .withCleanupLeasesUponShardCompletion(kcc.CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY)
      .withParentShardPollIntervalMillis(kcc.PARENT_SHARD_POLL_INTERVAL)
      .withShardSyncIntervalMillis(kcc.SHARD_SYNC_INTERVAL)
      .withTaskBackoffTimeMillis(kcc.BACKOFF_INTERVAL)
      .withMetricsBufferTimeMillis(kcc.CLOUDWATCH_BUFFER_TIME)
      .withMetricsMaxQueueSize(kcc.CLOUDWATCH_MAX_QUEUE_SIZE)
      .withUserAgent(
        kcc.APP_NAME                  + ","
          + kcc.CONNECTOR_DESTINATION + ","
          + KinesisConnectorConfiguration.KINESIS_CONNECTOR_USER_AGENT
      )
      .withRegionName(kcc.REGION_NAME)
      .withInitialLeaseTableReadCapacity(leaseTableInitialReadCapacity.getOrElse(DEFAULT_LEASE_TABLE_RCU))
      .withInitialLeaseTableWriteCapacity(leaseTableInitialWriteCapacity.getOrElse(DEFAULT_LEASE_TABLE_WCU))
      .withDynamoDBEndpoint(kcc.DYNAMODB_ENDPOINT)
    timestamp
      .filter(_ => initialPosition == "AT_TIMESTAMP")
      .map(cfg.withTimestampAtInitialPositionInStream(_))
      .getOrElse(cfg.withInitialPositionInStream(kcc.INITIAL_POSITION_IN_STREAM))
  }

  /**
   * Initialize the Amazon Kinesis Client Library configuration and worker with metrics factory
   *
   * @param kinesisConnectorConfiguration Amazon Kinesis connector configuration
   * @param metricFactory would be used to emit metrics in Amazon Kinesis Client Library
   */
  override def initialize(
    kinesisConnectorConfiguration: KinesisConnectorConfiguration,
    metricFactory: IMetricsFactory
  ): Unit = {
    val kinesisClientLibConfiguration = getKCLConfig(initialPosition, initialTimestamp, kinesisConnectorConfiguration)

    if (!kinesisConnectorConfiguration.CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST) {
      LOG.warn(
        "The false value of callProcessRecordsEvenForEmptyList will be ignored. It must be set to true for the bufferTimeMillisecondsLimit to work correctly."
      )
    }

    if (kinesisConnectorConfiguration.IDLE_TIME_BETWEEN_READS > kinesisConnectorConfiguration.BUFFER_MILLISECONDS_LIMIT) {
      LOG.warn(
        "idleTimeBetweenReads is greater than bufferTimeMillisecondsLimit. For best results, ensure that bufferTimeMillisecondsLimit is more than or equal to idleTimeBetweenReads "
      )
    }

    // If a metrics factory was specified, use it.
    worker = if (metricFactory != null) {
      new Worker(getKinesisConnectorRecordProcessorFactory, kinesisClientLibConfiguration, metricFactory)
    } else {
      new Worker(getKinesisConnectorRecordProcessorFactory, kinesisClientLibConfiguration)
    }
    LOG.info(getClass().getSimpleName() + " worker created")
  }

  initialize(convertConfig(streamLoaderConfig), null)

  def getKinesisConnectorRecordProcessorFactory =
    new KinesisConnectorRecordProcessorFactory[A, B](kinesisConnectorPipeline, convertConfig(streamLoaderConfig))

}
