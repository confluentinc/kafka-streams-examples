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

import java.util.Properties

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test based on [[WordCountLambdaExample]], using TopologyTestDriver.
  *
  * See [[WordCountLambdaExample]] for further documentation.
  * See [[WordCountLambdaIntegrationTest]] for the equivalent Java example.
  */
class WordCountScalaIntegrationTest extends AssertionsForJUnit {

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Test
  def shouldCountWords() {
    val inputTextLines: Seq[String] = Seq(
      "Hello Kafka Streams",
      "All streams lead to Kafka",
      "Join Kafka Summit"
    )

    val expectedWordCounts: Map[String, Long] = Map(
      ("hello", 1L),
      ("all", 1L),
      ("streams", 2L),
      ("lead", 1L),
      ("to", 1L),
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
      IntegrationTestScalaUtils.produceValuesSynchronously[String](inputTopic, inputTextLines, topologyTestDriver)

      // Step 3: Validate the output
      val actualWordCounts = IntegrationTestScalaUtils.drainTableOutput[String, Long](outputTopic, topologyTestDriver)
      assert(actualWordCounts === expectedWordCounts)
    } finally {
      topologyTestDriver.close()
    }
  }

  def createTopology(): StreamsBuilder = {
    val builder = new StreamsBuilder
    val textLines: KStream[Array[Byte], String] = builder.stream[Array[Byte], String](inputTopic)
    val wordCounts: KTable[String, Long] = textLines
        .flatMapValues(value => value.toLowerCase.split("\\W+"))
        .groupBy((_, word) => word)
        .count()
    wordCounts.toStream.to(outputTopic)
    builder
  }

  def createTopologyConfiguration(): Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-integration-test")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
    p
  }

}