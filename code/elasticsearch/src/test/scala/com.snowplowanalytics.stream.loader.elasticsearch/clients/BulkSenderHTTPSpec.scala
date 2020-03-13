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

// Amazon
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import utils.CredentialsLookup

// elastic4s
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.embedded.LocalNode

// scalaz
import scalaz._

// specs2
import org.specs2.mutable.Specification

class BulkSenderHTTPSpec extends Specification {
  val node = LocalNode("es", System.getProperty("java.io.tmpdir"))
  node.start()
  val client = node.elastic4sclient()
  val creds = CredentialsLookup.getCredentialsProvider("a", "s", Some("arnRole"), Some("stsRegion"))
  val username:Option[String] = None: Option[String]
  val password:Option[String] = None: Option[String]


  /*val sender = new BulkSenderHTTP("region", false, node.ip, node.port, username, password, false, 1000L, 1, None, None,creds )
  val index = "idx"
  client.execute(createIndex(index)).await

  "sendToElasticsearch" should {
    "successfully send stuff" in {
      val data = List(("a", Success(new ElasticsearchObject(index, "t", "i", """{"s":"json"}"""))))
      sender.send(data) must_== List.empty
      // eventual consistency
      Thread.sleep(1000)
      client.execute(search(index)).await.hits.head.sourceAsString must_== """{"s":"json"}"""
    }

    "report old failures" in {
      val data = List(("a", Failure(NonEmptyList("f"))))
      sender.send(data) must_== List(("a", Failure(NonEmptyList("f"))))
    }
  }*/
}