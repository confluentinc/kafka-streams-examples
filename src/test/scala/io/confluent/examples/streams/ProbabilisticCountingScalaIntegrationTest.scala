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

import io.confluent.examples.streams.IntegrationTestUtils.NothingSerde
import io.confluent.examples.streams.algebird.{CMSStore, CMSStoreBuilder, ProbabilisticCounter}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

import _root_.scala.collection.JavaConverters._

/**
  * End-to-end integration test that demonstrates how to probabilistically count items in an input stream.
  *
  * This example uses a custom state store implementation, [[CMSStore]], that is backed by a
  * Count-Min Sketch data structure.  The algorithm is WordCount.
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

    //
    // Step 1: Configure and start the processor topology.
    //
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "probabilistic-counting-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
      p
    }

    val builder = new StreamsBuilder
    val cmsStoreName = "cms-store"
    builder.addStateStore(createCMSStoreBuilder(cmsStoreName))

    // Read the input from Kafka.
    val textLines: KStream[String, String] = builder.stream[String, String](inputTopic)

    // previously:   def transform[K1, V1](transformer: Transformer[K, V, (K1, V1)],
    val approximateWordCounts: KStream[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .transform(() => new ProbabilisticCounter(cmsStoreName), cmsStoreName)

    // Write the results back to Kafka.
    approximateWordCounts.to(outputTopic)

    val topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)

    try {
      //
      // Step 2: Publish some input text lines.
      //
      IntegrationTestUtils.produceKeyValuesSynchronously(
        inputTopic,
        inputTextLines.map(v => new KeyValue(null, v)).asJava,
        topologyTestDriver,
        new NothingSerde[Null],
        new StringSerializer
      )

      //
      // Step 3: Verify the application's output data.
      //
      val actualWordCounts =
      IntegrationTestUtils.drainTableOutput(outputTopic, topologyTestDriver, new StringDeserializer, new LongDeserializer)

      // Note: This example only processes a small amount of input data, for which the word counts
      // will actually be exact counts.  However, for large amounts of input data we would expect to
      // observe approximate counts (where the approximate counts would be >= true exact counts).
      assertThat(actualWordCounts).isEqualTo(expectedWordCounts.asJava)
    } finally {
      topologyTestDriver.close()
    }
  }

  private def createCMSStoreBuilder(cmsStoreName: String): CMSStoreBuilder[String] = {
    val changelogConfig: util.HashMap[String, String] = {
      val cfg = new java.util.HashMap[String, String]
      // The CMSStore's changelog will typically have rather few and small records per partition.
      // To improve efficiency we thus set a smaller log segment size than Kafka's default of 1GB.
      val segmentSizeBytes = (20 * 1024 * 1024).toString
      cfg.put("segment.bytes", segmentSizeBytes)
      cfg
    }
    new CMSStoreBuilder[String](cmsStoreName, Serdes.String())
      .withLoggingEnabled(changelogConfig)
  }

}
