/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams

import java.util
import java.util.Properties

import io.confluent.examples.streams.algebird.{CMSStoreBuilder, ProbabilisticCounter}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that demonstrates how to probabilistically count items in an input stream.
  *
  * This example uses a custom state store implementation, [[io.confluent.examples.streams.algebird.CMSStore]],
  * that is backed by a Count-Min Sketch data structure.  The algorithm is WordCount.
  */
class ProbabilisticCountingScalaIntegrationTest extends AssertionsForJUnit {

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Test
  def shouldProbabilisticallyCountWords() {
    val inputTextLines: Seq[String] = Seq(
      "Hello Kafka Streams",
      "All streams lead to Kafka",
      "Join Kafka Summit"
    )

    val expectedWordCounts: Map[String, Long] = Map(
      ("hello", 1L),
      ("kafka", 1L),
      ("streams", 1L),
      ("all", 1L),
      ("streams", 2L),
      ("lead", 1L),
      ("to", 1L),
      ("kafka", 2L),
      ("join", 1L),
      ("kafka", 3L),
      ("summit", 1L)
    )

    // Step 1: Create the topology and its configuration
    val builder: StreamsBuilder = createTopology()
    val streamsConfiguration = createTopologyConfiguration()

    val topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)
    try {
      // Step 2: Write the input
      import IntegrationTestScalaUtils._
      IntegrationTestScalaUtils.produceValuesSynchronously(inputTopic, inputTextLines, topologyTestDriver)

      // Step 3: Validate the output
      val actualWordCounts: Map[String, Long] =
        IntegrationTestScalaUtils.drainTableOutput[String, Long](outputTopic, topologyTestDriver)

      // Note: This example only processes a small amount of input data, for which the word counts
      // will actually be exact counts.  However, for large amounts of input data we would expect to
      // observe approximate counts (where the approximate counts would be >= true exact counts).
      assert(actualWordCounts === expectedWordCounts)
    } finally {
      topologyTestDriver.close()
    }
  }

  def createTopology(): StreamsBuilder = {

    def createCMSStoreBuilder(cmsStoreName: String): CMSStoreBuilder[String] = {
      val changelogConfig: util.HashMap[String, String] = {
        val cfg = new java.util.HashMap[String, String]
        // The CMSStore's changelog will typically have rather few and small records per partition.
        // To improve efficiency we thus set a smaller log segment size than Kafka's default of 1GB.
        val segmentSizeBytes = (20 * 1024 * 1024).toString
        cfg.put("segment.bytes", segmentSizeBytes)
        cfg
      }
      new CMSStoreBuilder[String](cmsStoreName, Serdes.String()).withLoggingEnabled(changelogConfig)
    }

    val builder = new StreamsBuilder
    val cmsStoreName = "cms-store"
    builder.addStateStore(createCMSStoreBuilder(cmsStoreName))
    val textLines: KStream[String, String] = builder.stream[String, String](inputTopic)
    val approximateWordCounts: KStream[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .transform(() => new ProbabilisticCounter(cmsStoreName), cmsStoreName)
    approximateWordCounts.to(outputTopic)
    builder
  }

  def createTopologyConfiguration(): Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "probabilistic-counting-scala-integration-test")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
    p
  }

}