package sinks

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import model.Config.Kafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

class KafkaSink(broker:String,topic:String) extends ISink {

  val rnd = new Random()
  val props = new Properties()
  props.put("metadata.broker.list", broker)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[Option[String], Array[Byte]](props)

  override def store(output:String, key: Option[String], good: Boolean): Unit = {
    val record = new ProducerRecord[Option[String], Array[Byte]](topic, key, output.getBytes(UTF_8))
    producer.send(record)
  }
}