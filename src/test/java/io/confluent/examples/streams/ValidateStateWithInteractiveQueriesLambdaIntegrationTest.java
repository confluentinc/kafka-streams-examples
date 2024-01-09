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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Demonstrates how to validate an application's expected state through interactive queries.
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class ValidateStateWithInteractiveQueriesLambdaIntegrationTest {

  @Test
  public void shouldComputeMaxValuePerKey() throws Exception {
    // A user may be listed multiple times.
    final List<KeyValue<String, Long>> inputUserClicks = Arrays.asList(
      new KeyValue<>("alice", 13L),
      new KeyValue<>("bob", 4L),
      new KeyValue<>("chao", 25L),
      new KeyValue<>("bob", 19L),
      new KeyValue<>("chao", 56L),
      new KeyValue<>("alice", 78L),
      new KeyValue<>("alice", 40L),
      new KeyValue<>("bob", 3L)
    );

    final Map<String, Long> expectedMaxClicksPerUser = new HashMap<String, Long>() {
      {
        put("alice", 78L);
        put("bob", 19L);
        put("chao", 56L);
      }
    };

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "validating-with-interactive-queries-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final String inputTopic = "inputTopic";

    final KStream<String, Long> stream = builder.stream(inputTopic);

    // rolling MAX() aggregation
    final String maxStore = "max-store";
    stream.groupByKey().aggregate(
      () -> Long.MIN_VALUE,
      (aggKey, value, aggregate) -> Math.max(value, aggregate),
      Materialized.as(maxStore)
    );

    // windowed MAX() aggregation
    final String maxWindowStore = "max-window-store";
    stream.groupByKey()
      .windowedBy(TimeWindows.of(Duration.ofMinutes(1L)).grace(Duration.ZERO))
      .aggregate(
        () -> Long.MIN_VALUE,
        (aggKey, value, aggregate) -> Math.max(value, aggregate),
        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(maxWindowStore).withRetention(Duration.ofMinutes(5L)));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input topic.
      //
      final TestInputTopic<String, Long> input = topologyTestDriver
        .createInputTopic(inputTopic,
                          new StringSerializer(),
                          new LongSerializer());
      //
      // Step 3: Produce some input data to the input topic.
      //
      input.pipeKeyValueList(inputUserClicks);

      //
      // Step 4: Validate the application's state by interactively querying its state stores.
      //
      final ReadOnlyKeyValueStore<String, Long> keyValueStore =
        topologyTestDriver.getKeyValueStore(maxStore);

      final ReadOnlyWindowStore<String, Long> windowStore =
        topologyTestDriver.getWindowStore(maxWindowStore);

      IntegrationTestUtils.assertThatKeyValueStoreContains(keyValueStore, expectedMaxClicksPerUser);
      IntegrationTestUtils.assertThatOldestWindowContains(windowStore, expectedMaxClicksPerUser);
    }
  }

}
