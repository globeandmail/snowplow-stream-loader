/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
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

package emitters

// Java
import java.util.Properties

import com.snowplowanalytics.stream.loader.EmitterJsonInput
import org.slf4j.Logger

// Scala
import scala.collection.mutable.ListBuffer

// AWS libs
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer
import com.amazonaws.services.kinesis.connectors.{KinesisConnectorConfiguration, UnmodifiableBuffer}

// Scala
import scala.collection.JavaConverters._

// Scalaz
import scalaz.Scalaz._

// Specs2
import org.specs2.mutable.Specification

// This project
import clients.BulkSender
import sinks._

class MockBulkSender extends BulkSender[EmitterJsonInput] {
  override val log: Logger =  null
  override val maxAttempts: Int = 2
  override val maxConnectionWaitTimeMs: Long = 2
  var sentRecords: List[EmitterJsonInput] = List.empty
  var callCount: Int = 0
  val calls: ListBuffer[List[EmitterJsonInput]] = new ListBuffer
  override def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = {
    sentRecords = sentRecords ::: records
    callCount += 1
    calls += records
    List.empty
  }
  override def close() = {}
  override def logHealth(): Unit = ()
  override val tracker = None
}

class KinesisElasticsearchEmitterSpec extends Specification {
/*
  "The emitter" should {
    "return all invalid records" in {

      val fakeSender = new BulkSender[EmitterJsonInput] {
        override val log: Logger =  null
        override val maxAttempts: Int = 2
        override val maxConnectionWaitTimeMs: Long = 2
        override def send(records: List[EmitterJsonInput]): List[EmitterJsonInput] = List.empty
        override def close(): Unit = ()
        override def logHealth(): Unit = ()
        override val tracker = None
      }

      val kcc = new KinesisConnectorConfiguration(new Properties, new DefaultAWSCredentialsProviderChain)
      val eem = new Emitter(fakeSender, None, new StdouterrSink, 10, 10)

      val validInput: EmitterJsonInput = "good" -> new ElasticsearchObject("index", "type", "{}").success
      val invalidInput: EmitterJsonInput = "bad" -> "malformed event".failureNel

      val input = List(validInput, invalidInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      val actual = eem.emit(ub)

      actual must_== List(invalidInput).asJava
    }

    "send multiple records in seperate requests where single record size > buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1000")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = new MockBulkSender
      val eem = new ElasticsearchEmitter(ess, None, new StdouterrSink, 10, 10)


      val validInput: EmitterJsonInput = "good" -> new ElasticsearchObject("index" * 10000, "type", "{}").success

      val input = List.fill(50)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 50
      forall (ess.calls) { c => c.length mustEqual 1 }
    }

    "send a single record in 1 request where record size > buffer bytes size " in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1000")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = new MockBulkSender
      val eem = new ElasticsearchEmitter(ess, None, new StdouterrSink, 10, 10)

      val validInput: EmitterJsonInput = "good" -> new ElasticsearchObject("index" * 10000, "type", "{}").success

      val input = List(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall (ess.calls) { c => c.length mustEqual 1 }
    }

    "send multiple records in 1 request where total byte size < buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1048576")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = new MockBulkSender
      val eem = new ElasticsearchEmitter(ess, None, new StdouterrSink, 10, 10)

      val validInput: EmitterJsonInput = "good" -> new ElasticsearchObject("index", "type", "{}").success

      val input = List.fill(50)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall (ess.calls) { c => c.length mustEqual 50 }
    }

    "send a single record in 1 request where single record size < buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1048576")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = new MockBulkSender
      val eem = new ElasticsearchEmitter(ess, None, new StdouterrSink, 10, 10)

      val validInput: EmitterJsonInput = "good" -> new ElasticsearchObject("index", "type", "{}").success

      val input = List(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall (ess.calls) { c => c.length mustEqual 1 }
    }

    "send multiple records in batches where single record byte size < buffer size and total byte size > buffer size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "200")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)
      val ess = new MockBulkSender
      val eem = new ElasticsearchEmitter(ess, None, new StdouterrSink, 10, 10)

      // record size is 95 bytes
      val validInput: EmitterJsonInput = "good" -> new ElasticsearchObject("index", "type", "{}").success

      val input = List.fill(20)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterJsonInput](kcc, input.asJava)
      val ub = new UnmodifiableBuffer[EmitterJsonInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 10 // 10 buffers of 2 records each
      forall (ess.calls) { c => c.length mustEqual 2 }
    }
  }*/

}
