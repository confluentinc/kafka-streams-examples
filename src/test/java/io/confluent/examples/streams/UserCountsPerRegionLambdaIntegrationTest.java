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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates how aggregations on a KTable produce the expected
 * results even though new data is continuously arriving in the KTable's input topic in Kafka.
 * <p>
 * The use case we implement is to continuously compute user counts per region based on location
 * updates that are sent to a Kafka topic.  What we want to achieve is to always have the
 * latest and correct user counts per region even as users keep moving between regions while our
 * stream processing application is running (imagine, for example, that we are tracking passengers
 * on air planes).  More concretely,  whenever a new messages arrives in the Kafka input topic that
 * indicates a user moved to a new region, we want the effect of 1) reducing the user's previous
 * region by 1 count and 2) increasing the user's new region by 1 count.
 * <p>
 * You could use the code below, for example, to create a real-time heat map of the world where
 * colors denote the current number of users in each area of the world.
 * <p>
 * This example is related but not equivalent to {@link UserRegionLambdaExample}.
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class UserCountsPerRegionLambdaIntegrationTest {
  private static final String inputTopic = "input-topic";
  private static final String outputTopic = "output-topic";

  @Test
  public void shouldCountUsersPerRegion() {
    // Input: Region per user (multiple records allowed per user).
    final List<KeyValue<String, String>> userRegionRecords = Arrays.asList(

      // This first record for Alice tells us that she is currently in Asia.
      new KeyValue<>("alice", "asia"),
      // First record for Bob.
      new KeyValue<>("bob", "europe"),
      // This second record for Alice tells us that her latest location is Europe.  Combining the
      // information in this record with the previous record for Alice, we know that she has moved
      // from Asia to Europe;  in other words, it's a location update for Alice.
      new KeyValue<>("alice", "europe"),
      // Second record for Bob, who moved from Europe to Asia (i.e. the opposite direction of Alice).
      new KeyValue<>("bob", "asia")
    );

    final Map<String, Long> expectedUsersPerRegion = mkMap(
      mkEntry("europe", 1L), // in the end, Alice is in europe
      mkEntry("asia", 1L)    // in the end, Bob is in asia
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-regions-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final StreamsBuilder builder = new StreamsBuilder();

    final KTable<String, String> userRegionsTable = builder.table(inputTopic);

    final KTable<String, Long> usersPerRegionTable = userRegionsTable

      // no need to specify explicit serdes because the resulting key and value types match our default serde settings
      .groupBy((userId, region) -> KeyValue.pair(region, region))
      .count();

    usersPerRegionTable.toStream().to(outputTopic, Produced.with(stringSerde, longSerde));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<String, String> input = topologyTestDriver
          .createInputTopic(inputTopic,
                            new StringSerializer(),
                            new StringSerializer());
      final TestOutputTopic<String, Long> output = topologyTestDriver
        .createOutputTopic(outputTopic,
                           new StringDeserializer(),
                           new LongDeserializer());

      //
      // Step 3: Publish user-region information.
      //
      input.pipeKeyValueList(userRegionRecords);

      //
      // Step 4: Verify the application's output data.
      //
      assertThat(output.readKeyValuesToMap(), equalTo(expectedUsersPerRegion));
    }
  }

}
