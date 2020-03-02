/*
 * © Copyright 2020 The Globe and Mail
 */
package loader
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

// Java
import java.io.File

import com.amazonaws.auth.AWSCredentialsProvider
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.stream.loader.model.Config._
import scopt.OptionParser
import com.snowplowanalytics.stream.loader.sinks._
import utils.{CredentialsLookup, SnowplowTracking}

// Config
import com.typesafe.config.ConfigFactory

// Scalaz
import scalaz._

// Pureconfig
import pureconfig._

/**
 * Main entry point for the Elasticsearch sink
 */
trait StreamLoaderApp extends App {

  val executor: Validation[String, Runnable]
  lazy val arguments = args

  private def parseConfig(): Option[StreamLoaderConfig] = {
    val projectName = "snowplow-stream-loader"
    case class FileConfig(config: File = new File("."))
    val parser: OptionParser[FileConfig] = new scopt.OptionParser[FileConfig](projectName) {
      head(projectName, com.snowplowanalytics.stream.loader.generated.Settings.version)
      help("help")
      version("version")
      opt[File]("config")
        .required()
        .valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(config = f))
        .validate(
          f =>
            if (f.exists) {
              success
            }
            else {
              failure(s"Configurationfile $f does not exist")
            }
        )
    }

    val config = parser.parse(arguments, FileConfig()) match {
      case Some(c) => ConfigFactory.parseFile(c.config).resolve()
      case None    => ConfigFactory.empty()
    }

    if (config.isEmpty()) {
      System.err.print("Empty configuration file")
      System.exit(1)
    }

    implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

    implicit val queueConfigHint: FieldCoproductHint[QueueConfig] = new FieldCoproductHint[QueueConfig]("enabled")

    val streamLoaderConf = loadConfig[StreamLoaderConfig](config) match {
      case Left(e) =>
        System.err.print(s"configuration error: $e")
        System.exit(1)
        None
      case Right(c) => Some(c)
    }

    streamLoaderConf
  }

  lazy val config: StreamLoaderConfig = parseConfig().get
  lazy val credentials: AWSCredentialsProvider = CredentialsLookup.getCredentialsProvider(
    config.aws.accessKey,
    config.aws.secretKey,
    config.aws.arnRole,
    config.aws.stsRegion
  )
  lazy val tracker: Option[Tracker] = config.monitoring.map(e => SnowplowTracking.initializeTracker(e.snowplow))

  lazy val goodSink: Option[ISink] = config.sink.good match {
    case "stdout"        => Some(new StdouterrSink)
    case "elasticsearch" => None
    case "postgres"      => None
    case "s3"            => None
    case "kinesis" =>
      config.queueConfig match {
        case streamConfig: Kinesis =>
          config.kinesis match {
            case Some(kinesisConfig) =>
              val kinesisCredentials = CredentialsLookup.getCredentialsProvider(
                kinesisConfig.kinesisAccessKey,
                kinesisConfig.kinesisSecretKey,
                kinesisConfig.kinesisArnRole,
                kinesisConfig.kinesisStsRegion
              )
              val kinesisSinkFilter = kinesisConfig.filterAppId
              Some(
                new KinesisSink(
                  kinesisCredentials,
                  streamConfig.endpoint,
                  streamConfig.region,
                  config.streams.outStreamName,
                  kinesisSinkFilter
                )
              )
          }

        case _ => throw new RuntimeException("No Kinesis configuration to be used for good stream")
      }
  }

  lazy val badSink: ISink = config.sink.bad match {
    case "stderr" => new StdouterrSink
    case "nsq" =>
      config.queueConfig match {
        case streamConfig: Nsq =>
          new NsqSink(streamConfig.host, streamConfig.port, config.streams.outStreamName)
        case _ => throw new RuntimeException("No Nsq configuration to be used for bad stream")
      }
    case "kafka" =>
      config.queueConfig match {
        case streamConfig: Kafka =>
          new KafkaSink(config.kafka.get.broker,config.kafka.get.badProducerTopic)
        case _ => throw new RuntimeException("No kafka configuration to be used for bad stream")
      }

    case "none" => new NullSink
    case "kinesis" =>
      config.queueConfig match {
        case streamConfig: Kinesis =>
          val kinesisCredentials: AWSCredentialsProvider = config.kinesis match {
            case Some(kinesisConfig) =>
              CredentialsLookup.getCredentialsProvider(
                kinesisConfig.kinesisAccessKey,
                kinesisConfig.kinesisSecretKey,
                kinesisConfig.kinesisArnRole,
                kinesisConfig.kinesisStsRegion
              )
            case None => credentials
          }
          new KinesisSink(
            kinesisCredentials,
            streamConfig.endpoint,
            streamConfig.region,
            config.streams.outStreamName,
            None
          )
        case _ => throw new RuntimeException("No Kinesis configuration to be used for bad stream")
      }

  }

  def run(): Unit =
    executor.fold(
      err => throw new RuntimeException(err),
      exec => {
        tracker foreach { t =>
          SnowplowTracking.initializeSnowplowTracking(t)
        }
        exec.run()
        // If the stream cannot be found, the KCL's "cw-metrics-publisher" thread will prevent the
        // application from exiting naturally so we explicitly call System.exit.
        // This does not apply to NSQ because NSQ consumer is non-blocking and fall here
        // right after consumer.start()
        config.source match {
          case "kinesis" => System.exit(1)
          case "stdin"   => System.exit(1)
          // do anything
          case "nsq" =>
          case "kafka"=>
        }
      }
    )

}
