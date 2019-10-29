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

import io.confluent.examples.streams.IntegrationTestUtils.NothingSerde
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.junit._
import org.scalatest.junit.AssertionsForJUnit
import org.assertj.core.api.Assertions.assertThat

import scala.collection.JavaConverters._

/**
  * End-to-end integration test based on [[WordCountLambdaExample]], using an embedded Kafka cluster.
  *
  * See [[WordCountLambdaExample]] for further documentation.
  *
  * See [[WordCountLambdaIntegrationTest]] for the equivalent Java example.
  *
  * Note: We intentionally use JUnit4 (wrapped by ScalaTest) for implementing this Scala integration
  * test so it is easier to compare this Scala code with the equivalent Java code at
  * StreamToTableJoinIntegrationTest.  One difference is that, to simplify the Scala/Junit integration, we
  * switched from BeforeClass (which must be `static`) to Before as well as from @ClassRule (which
  * must be `static` and `public`) to a workaround combination of `@Rule def` and a `private val`.
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

    //
    // Step 1: Configure and start the processor topology.
    //
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
      p
    }

    val builder = new StreamsBuilder

    val textLines: KStream[Array[Byte], String] = builder.stream[Array[Byte], String](inputTopic)

    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(value => value.toLowerCase.split("\\W+"))
      .groupBy((_, word) => word)
      .count()

    wordCounts.toStream.to(outputTopic)


    val topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)

    try {
      //
      // Step 2: Setup input and output topics.
      //
      val input = topologyTestDriver.createInputTopic(inputTopic,
                                                      new NothingSerde[Null],
                                                      new StringSerializer)
      val output = topologyTestDriver.createOutputTopic(outputTopic,
                                                        new StringDeserializer,
                                                        new LongDeserializer)
      //
      // Step 3: Publish some input text lines.
      //
      input.pipeKeyValueList(inputTextLines.map(v => new KeyValue(null, v)).asJava)

      //
      // Step 4: Verify the application's output data.
      //
      assertThat(output.readKeyValuesToMap()).isEqualTo(expectedWordCounts.asJava)
    } finally {
      topologyTestDriver.close()
    }
  }
}