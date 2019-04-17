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
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that shows how to aggregate messages via `groupByKey()` and `reduce()`.
  *
  * This example can be adapted to structured (nested) data formats such as Avro or JSON in case you need to concatenate
  * only certain field(s) in the input.
  *
  * See [[ReduceTest]] for the equivalent Java example.
  */
class ReduceScalaTest extends AssertionsForJUnit {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Test
  def shouldConcatenateWithReduce() {
    val inputRecords: Seq[KeyValue[Int, String]] = Seq(
      (456, "stream"),
      (123, "hello"),
      (123, "world"),
      (456, "all"),
      (123, "kafka"),
      (456, "the"),
      (456, "things"),
      (123, "streams")
    )

    val expectedOutput: Map[Int, String] = Map(
      456 -> "stream all the things",
      123 -> "hello world kafka streams"
    )

    // Step 1: Create the topology and its configuration
    val builder: StreamsBuilder = createTopology()
    val streamsConfiguration = createTopologyConfiguration()

    val topologyTestDriver: TopologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)
    try {
      // Step 2: Write the input
      import collection.JavaConverters._
      IntegrationTestUtils.produceKeyValuesSynchronously(
        inputTopic,
        inputRecords.map(kv => new KeyValue(kv.key: java.lang.Integer, kv.value)).asJava,
        topologyTestDriver,
        new IntegerSerializer,
        new StringSerializer)

      // Step 3: Validate the output
      val actualOutput: util.Map[java.lang.Integer, String] = IntegrationTestUtils.drainTableOutput(
        outputTopic, topologyTestDriver, new IntegerDeserializer, new StringDeserializer)
      assert(actualOutput.asScala === expectedOutput)
    } finally {
      topologyTestDriver.close()
    }
  }

  def createTopology(): StreamsBuilder = {
    val builder = new StreamsBuilder
    val input: KStream[Int, String] = builder.stream[Int, String](inputTopic)
    val concatenated: KTable[Int, String] = input.groupByKey.reduce((v1, v2) => v1 + " " + v2)
    concatenated.toStream.to(outputTopic)
    builder
  }

  def createTopologyConfiguration(): Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce-scala-test")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, TimeUnit.SECONDS.toMillis(10).toString)
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
    p
  }

}