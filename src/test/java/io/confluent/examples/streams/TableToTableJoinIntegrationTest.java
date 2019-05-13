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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between two KTables.
 * We also show how to assign a state store to the joined table, which is required to make it
 * accessible for interactive queries.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class TableToTableJoinIntegrationTest {

  private static final String userRegionTopic = "user-region-topic";
  private static final String userLastLoginTopic = "user-last-login-topic";
  private static final String outputTopic = "output-topic";

  @Test
  public void shouldJoinTwoTables() {
    // Input: Region per user (multiple records allowed per user).
    final List<KeyValue<String, String>> userRegionRecords = Arrays.asList(
      new KeyValue<>("alice", "asia"),
      new KeyValue<>("bob", "europe"),
      new KeyValue<>("alice", "europe"),
      new KeyValue<>("charlie", "europe"),
      new KeyValue<>("bob", "asia")
    );

    // Input 2: Timestamp of last login per user (multiple records allowed per user)
    final List<KeyValue<String, Long>> userLastLoginRecords = Arrays.asList(
      new KeyValue<>("alice", 1485500000L),
      new KeyValue<>("bob", 1485520000L),
      new KeyValue<>("alice", 1485530000L),
      new KeyValue<>("bob", 1485560000L)
    );

    final List<KeyValue<String, String>> expectedResults = Arrays.asList(
      new KeyValue<>("alice", "europe/1485500000"),
      new KeyValue<>("bob", "asia/1485520000"),
      new KeyValue<>("alice", "europe/1485530000"),
      new KeyValue<>("bob", "asia/1485560000")
    );

    final List<KeyValue<String, String>> expectedResultsForJoinStateStore = Arrays.asList(
      new KeyValue<>("alice", "europe/1485530000"),
      new KeyValue<>("bob", "asia/1485560000")
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-table-join-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final StreamsBuilder builder = new StreamsBuilder();
    final KTable<String, String> userRegions = builder.table(userRegionTopic);
    final KTable<String, Long> userLastLogins = builder.table(userLastLoginTopic, Consumed.with(stringSerde, longSerde));

    final String storeName = "joined-store";
    userRegions.join(userLastLogins,
      (regionValue, lastLoginValue) -> regionValue + "/" + lastLoginValue,
      Materialized.as(storeName))
      .toStream()
      .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Publish user regions.
      //
      IntegrationTestUtils.produceKeyValuesSynchronously(
        userRegionTopic,
        userRegionRecords,
        topologyTestDriver,
        new StringSerializer(),
        new StringSerializer()
      );

      //
      // Step 3: Publish user's last login timestamps.
      //
      IntegrationTestUtils.produceKeyValuesSynchronously(
        userLastLoginTopic,
        userLastLoginRecords,
        topologyTestDriver,
        new StringSerializer(),
        new LongSerializer()
      );

      //
      // Step 4: Verify the application's output data.
      //
      final List<KeyValue<String, String>> actualResults = IntegrationTestUtils.drainStreamOutput(
        outputTopic,
        topologyTestDriver,
        new StringDeserializer(),
        new StringDeserializer()
      );

      // Verify the (local) state store of the joined table.
      // For a comprehensive demonstration of interactive queries please refer to KafkaMusicExample.
      final ReadOnlyKeyValueStore<String, String> readOnlyKeyValueStore =
        topologyTestDriver.getKeyValueStore(storeName);
      final KeyValueIterator<String, String> keyValueIterator = readOnlyKeyValueStore.all();
      assertThat(keyValueIterator).containsExactlyElementsOf(expectedResultsForJoinStateStore);

      assertThat(actualResults).containsExactlyElementsOf(expectedResults);
    }
  }
}
