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
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that shows how to aggregate messages via `groupBy()` and `aggregate()`.
  *
  * See [[AggregateTest]] for the equivalent Java example.
  */
class AggregateScalaTest extends AssertionsForJUnit {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Test
  def shouldAggregate() {
    val inputValues: Seq[String] =
      Seq("stream", "all", "the", "things", "hi", "world", "kafka", "streams", "streaming")

    val expectedOutput: Map[String, Long] = Map(
      "a" -> 3L,
      "t" -> 9L,
      "h" -> 2L,
      "w" -> 5L,
      "k" -> 5L,
      "s" -> 22L,
    )

    // Step 1: Create the topology and its configuration
    val builder: StreamsBuilder = createTopology()
    val streamsConfiguration = createTopologyConfiguration()

    val topologyTestDriver: TopologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)
    try {
      // Step 2: Write the input
      import collection.JavaConverters._
      IntegrationTestUtils.produceValuesSynchronously(
        inputTopic, inputValues.asJava, topologyTestDriver, new StringSerializer)

      // Step 3: Validate the output
      val actualOutput: util.Map[String, java.lang.Long] = IntegrationTestUtils.drainTableOutput(
        outputTopic, topologyTestDriver, new StringDeserializer, new LongDeserializer)
      assert(actualOutput.asScala === expectedOutput)
    } finally {
      topologyTestDriver.close()
    }
  }

  def createTopology(): StreamsBuilder = {
    val builder = new StreamsBuilder
    val input: KStream[Array[Byte], String] = builder.stream[Array[Byte], String](inputTopic)
    val aggregated: KTable[String, Long] = input
        .groupBy(
          (key: Array[Byte], value: String) => Option(value) match {
            case Some(s) if s.nonEmpty => s.head.toString
            case _ => ""
          })
        .aggregate(0L)((aggKey: String, newValue: String, aggValue: Long) => aggValue + newValue.length)
    aggregated.toStream.to(outputTopic)
    builder
  }

  def createTopologyConfiguration(): Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-scala-test")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-config")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, TimeUnit.SECONDS.toMillis(10).toString)
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
    p
  }

}