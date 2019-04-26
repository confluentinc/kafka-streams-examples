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

import java.lang.{Long => JLong}
import java.util.Properties

import io.confluent.examples.streams.IntegrationTestUtils.NothingSerde
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced}
import org.apache.kafka.test.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

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
    // To convert between Scala's `Tuple2` and Streams' `KeyValue`.

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
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
      p
    }

    val stringSerde: Serde[String] = Serdes.String()
    val longSerde: Serde[JLong] = Serdes.Long()

    val builder: StreamsBuilder = new StreamsBuilder()

    // Construct a `KStream` from the input topic, where message values represent lines of text (for
    // the sake of this example, we ignore whatever may be stored in the message keys).
    val textLines: KStream[String, String] = builder.stream(inputTopic)

    // Scala-Java interoperability: to convert `scala.collection.Iterable` to  `java.util.Iterable`
    // in `flatMapValues()` below.
    import collection.JavaConverters.asJavaIterableConverter

    val wordCounts: KTable[String, JLong] = textLines
      .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable.asJava)
      // no need to specify explicit serdes because the resulting key and value types match our default serde settings
      .groupBy((_, word) => word)
      .count()

    wordCounts.toStream.to(outputTopic, Produced.`with`(stringSerde, longSerde))

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
      assertThat(actualWordCounts).isEqualTo(expectedWordCounts.asJava)
    } finally {
      topologyTestDriver.close()
    }
  }

}
