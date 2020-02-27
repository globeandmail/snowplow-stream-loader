/*
 * © Copyright 2020 The Globe and Mail
 */
package com.snowplowanalytics.stream.loader
package executors


import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.Executors

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import com.snowplowanalytics.stream.loader.clients.BulkSender
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.snowplowanalytics.stream.loader.transformers.eventTransformers._
import com.snowplowanalytics.stream.loader.emitter.Emitter
import com.snowplowanalytics.stream.loader.model.Config._
import model.JsonRecord
import scalaz.ValidationNel
import sinks.ISink

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


class KafkaSourceExecutor(streamType: StreamType,
                          goodSink: Option[ISink],
                          badSink: ISink,
                          postgresSender:BulkSender[EmitterJsonInput],
                          kafka: Kafka,
                          shardDateField: Option[String],
                          shardDateFormat: Option[String],
                          config:StreamLoaderConfig) extends Runnable  {

// value of shardDateField , shardDateFormat,goodSInk and badSink ??
  val properties: Properties = KafkaProcessorConfig(kafka.broker, kafka.groupId)
  val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](properties)
  val kafkaBufferSize: Long = config.streams.buffer.recordLimit
  val msgBuffer = new ListBuffer[EmitterJsonInput]()
  val emitter: Emitter = new Emitter(
    postgresSender,
    goodSink,
    badSink,
    config.streams.buffer.recordLimit,
    config.streams.buffer.byteLimit)

  val transformer: ITransformer[(String, ValidationNel[String, JsonRecord]), (String, ValidationNel[String, JsonRecord])] with StdinTransformer =
    streamType match {
      case Good => new EnrichedEventJsonTransformer(shardDateField, shardDateFormat)
      case PlainJson => new PlainJsonTransformer
      case Bad => new BadEventTransformer
    }


  private def KafkaProcessorConfig(broker:String,groupId:String) :Properties={
    val properties = new Properties()

    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.broker)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafka.groupId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    properties
  }

  override def run(): Unit = {

    consumer.subscribe(util.Arrays.asList(kafka.consumeTopic))
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val Thousand = 1000
          val record = consumer.poll(Duration.ofMillis(Thousand)).asScala
          for (data <- record.iterator)
            msgBuffer.synchronized {
              val emitterInput = transformer.consumeLine(data.value().map(_.toChar).mkString)
              msgBuffer += emitterInput

              if (msgBuffer.size == kafkaBufferSize) {
                val rejectedRecords = emitter.emit(msgBuffer.toList)
                emitter.fail(rejectedRecords.asJava)
                msgBuffer.clear()
              }
            }
        }
      }
    })

  }

}
