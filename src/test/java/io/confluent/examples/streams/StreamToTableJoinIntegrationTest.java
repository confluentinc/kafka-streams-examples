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
package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.confluent.examples.streams.IntegrationTestUtils.mkEntry;
import static io.confluent.examples.streams.IntegrationTestUtils.mkMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between a KStream and a
 * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
 *
 * See StreamToTableJoinScalaIntegrationTest for the equivalent Scala example.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class StreamToTableJoinIntegrationTest {

  private static final String userClicksTopic = "user-clicks";
  private static final String userRegionsTopic = "user-regions";
  private static final String outputTopic = "output-topic";

  /**
   * Tuple for a region and its associated number of clicks.
   */
  private static final class RegionWithClicks {

    private final String region;
    private final long clicks;

    RegionWithClicks(final String region, final long clicks) {
      if (region == null || region.isEmpty()) {
        throw new IllegalArgumentException("region must be set");
      }
      if (clicks < 0) {
        throw new IllegalArgumentException("clicks must not be negative");
      }
      this.region = region;
      this.clicks = clicks;
    }

    String getRegion() {
      return region;
    }

    long getClicks() {
      return clicks;
    }

  }

  @Test
  public void shouldCountClicksPerRegion() {
    // Input 1: Clicks per user (multiple records allowed per user).
    final List<KeyValue<String, Long>> userClicks = Arrays.asList(
      new KeyValue<>("alice", 13L),
      new KeyValue<>("bob", 4L),
      new KeyValue<>("chao", 25L),
      new KeyValue<>("bob", 19L),
      new KeyValue<>("dave", 56L),
      new KeyValue<>("eve", 78L),
      new KeyValue<>("alice", 40L),
      new KeyValue<>("fang", 99L)
    );

    // Input 2: Region per user (multiple records allowed per user).
    final List<KeyValue<String, String>> userRegions = Arrays.asList(
      new KeyValue<>("alice", "asia"),   /* Alice lived in Asia originally... */
      new KeyValue<>("bob", "americas"),
      new KeyValue<>("chao", "asia"),
      new KeyValue<>("dave", "europe"),
      new KeyValue<>("alice", "europe"), /* ...but moved to Europe some time later. */
      new KeyValue<>("eve", "americas"),
      new KeyValue<>("fang", "asia")
    );

    final Map<String, Long> expectedClicksPerRegion = mkMap(
      mkEntry("americas", 101L),
      mkEntry("europe", 109L),
      mkEntry("asia", 124L)
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final StreamsBuilder builder = new StreamsBuilder();

    // This KStream contains information such as "alice" -> 13L.
    //
    // Because this is a KStream ("record stream"), multiple records for the same user will be
    // considered as separate click-count events, each of which will be added to the total count.
    final KStream<String, Long> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, longSerde));

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
    final KTable<String, String> userRegionsTable = builder.table(userRegionsTopic);

    // Compute the number of clicks per region, e.g. "europe" -> 13L.
    //
    // The resulting KTable is continuously being updated as new data records are arriving in the
    // input KStream `userClicksStream` and input KTable `userRegionsTable`.
    final KTable<String, Long> clicksPerRegion = userClicksStream
        // Join the stream against the table.
        //
        // Null values possible: In general, null values are possible for region (i.e. the value of
        // the KTable we are joining against) so we must guard against that (here: by setting the
        // fallback region "UNKNOWN").  In this specific example this is not really needed because
        // we know, based on the test setup, that all users have appropriate region entries at the
        // time we perform the join.
        //
        // Also, we need to return a tuple of (region, clicks) for each user.  But because Java does
        // not support tuples out-of-the-box, we must use a custom class `RegionWithClicks` to
        // achieve the same effect.
        .leftJoin(userRegionsTable, (clicks, region) -> new RegionWithClicks(region == null ? "UNKNOWN" : region, clicks))
        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .map((user, regionWithClicks) -> new KeyValue<>(regionWithClicks.getRegion(), regionWithClicks.getClicks()))
        // Compute the total per region by summing the individual click counts per region.
        .groupByKey(Grouped.with(stringSerde, longSerde))
        .reduce(Long::sum);

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream().to(outputTopic, Produced.with(stringSerde, longSerde));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Publish user-region information.
      //
      // To keep this code example simple and easier to understand/reason about, we publish all
      // user-region records before any user-click records (cf. step 3).  In practice though,
      // data records would typically be arriving concurrently in both input streams/topics.
      IntegrationTestUtils.produceKeyValuesSynchronously(
        userRegionsTopic,
        userRegions,
        topologyTestDriver,
        new StringSerializer(),
        new StringSerializer()
      );

      //
      // Step 3: Publish some user click events.
      //
      IntegrationTestUtils.produceKeyValuesSynchronously(
        userClicksTopic,
        userClicks,
        topologyTestDriver,
        new StringSerializer(),
        new LongSerializer());

      //
      // Step 4: Verify the application's output data.
      //
      final Map<String, Long> actualClicksPerRegion =
        IntegrationTestUtils.drainTableOutput(
          outputTopic,
          topologyTestDriver,
          new StringDeserializer(),
          new LongDeserializer()
        );
      assertThat(actualClicksPerRegion).isEqualTo(expectedClicksPerRegion);
    }
  }

}
