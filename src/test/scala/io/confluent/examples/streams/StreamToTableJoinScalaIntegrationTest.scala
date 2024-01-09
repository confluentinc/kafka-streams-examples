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

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that demonstrates how to perform a join between a KStream and a
  * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
  *
  * See StreamToTableJoinIntegrationTest for the equivalent Java example.
  *
  * Note: We intentionally use JUnit4 (wrapped by ScalaTest) for implementing this Scala integration
  * test so it is easier to compare this Scala code with the equivalent Java code at
  * StreamToTableJoinIntegrationTest.  One difference is that, to simplify the Scala/Junit integration, we
  * switched from BeforeClass (which must be `static`) to Before as well as from @ClassRule (which
  * must be `static` and `public`) to a workaround combination of `@Rule def` and a `private val`.
  */
class StreamToTableJoinScalaIntegrationTest extends AssertionsForJUnit {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  private val userClicksTopic = "user-clicks"
  private val userRegionsTopic = "user-regions"
  private val outputTopic = "output-topic"

  @Test
  def shouldCountClicksPerRegion() {
    // Input 1: Clicks per user (multiple records allowed per user).
    val userClicks: Seq[KeyValue[String, Long]] = Seq(
      ("alice", 13L),
      ("bob", 4L),
      ("chao", 25L),
      ("bob", 19L),
      ("dave", 56L),
      ("eve", 78L),
      ("alice", 40L),
      ("fang", 99L)
    )

    // Input 2: Region per user (multiple records allowed per user).
    val userRegions: Seq[KeyValue[String, String]] = Seq(
      ("alice", "asia"), /* Alice lived in Asia originally... */
      ("bob", "americas"),
      ("chao", "asia"),
      ("dave", "europe"),
      ("alice", "europe"), /* ...but moved to Europe some time later. */
      ("eve", "americas"),
      ("fang", "asia")
    )

    val expectedClicksPerRegion: Map[String, Long] = Map(
      ("americas", 101L),
      ("europe", 109L),
      ("asia", 124L)
    )

    //
    // Step 1: Configure and start the processor topology.
    //
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
      p
    }

    val builder = new StreamsBuilder

    // This KStream contains information such as "alice" -> 13L.
    //
    // Because this is a KStream ("record stream"), multiple records for the same user will be
    // considered as separate click-count events, each of which will be added to the total count.
    val userClicksStream: KStream[String, Long] = builder.stream[String, Long](userClicksTopic)

    // This KTable contains information such as "alice" -> "europe".
    //
    // Because this is a KTable ("changelog stream"), only the latest value (here: region) for a
    // record key will be considered at the time when a new user-click record (see above) is
    // received for the `leftJoin` below.  Any previous region values are being considered out of
    // date.  This behavior is quite different to the KStream for user clicks above.
    //
    // For example, the user "alice" will be considered to live in "europe" (although originally she
    // lived in "asia") because, at the time her first user-click record is being received and
    // subsequently processed in the `leftJoin`, the latest region update for "alice" is "europe"
    // (which overrides her previous region value of "asia").
    val userRegionsTable: KTable[String, String] = builder.table[String, String](userRegionsTopic)

    // Compute the number of clicks per region, e.g. "europe" -> 13L.
    //
    // The resulting KTable is continuously being updated as new data records are arriving in the
    // input KStream `userClicksStream` and input KTable `userRegionsTable`.
    val userClicksJoinRegion: KStream[String, (String, Long)] = userClicksStream
      // Join the stream against the table.
      //
      // Null values possible: In general, null values are possible for region (i.e. the value of
      // the KTable we are joining against) so we must guard against that (here: by setting the
      // fallback region "UNKNOWN").  In this specific example this is not really needed because
      // we know, based on the test setup, that all users have appropriate region entries at the
      // time we perform the join.
      .leftJoin(userRegionsTable)((clicks, region) => (if (region == null) "UNKNOWN" else region, clicks))

    val clicksByRegion: KStream[String, Long] = userClicksJoinRegion
      .map { case (_, (region, clicks)) => (region, clicks) }

    val clicksPerRegion: KTable[String, Long] = clicksByRegion
      // Compute the total per region by summing the individual click counts per region.
      .groupByKey
      .reduce(_ + _)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopic)

    val topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)

    try {
      //
      // Step 2: Setup input and output topics.
      //
      import collection.JavaConverters._
      val regionInput = topologyTestDriver.createInputTopic(userRegionsTopic,
                                                            new StringSerializer,
                                                            new StringSerializer)
      val clickInput = topologyTestDriver.createInputTopic(userClicksTopic,
                                                           new StringSerializer,
                                                           new LongSerializer)
      val output = topologyTestDriver.createOutputTopic(outputTopic,
                                                        new StringDeserializer,
                                                        new LongDeserializer)

      //
      // Step 3: Publish user-region information.
      //
      // To keep this code example simple and easier to understand/reason about, we publish all
      // user-region records before any user-click records (cf. step 3).  In practice though,
      // data records would typically be arriving concurrently in both input streams/topics.
      regionInput.pipeKeyValueList(userRegions.asJava)

      //
      // Step 4: Publish some user click events.
      //
      clickInput.pipeKeyValueList(userClicks.map(kv => new KeyValue(kv.key, kv.value.asInstanceOf[java.lang.Long])).asJava)

      //
      // Step 5: Verify the application's output data.
      //
      assertThat(output.readKeyValuesToMap()).isEqualTo(expectedClicksPerRegion.asJava)
    } finally {
      topologyTestDriver.close()
    }
  }

}
