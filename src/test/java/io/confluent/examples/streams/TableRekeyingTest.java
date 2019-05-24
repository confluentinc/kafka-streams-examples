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

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How to re-key (re-partition) a KTable, resulting in a new KTable.
 * This specific example flips keys and values in the original table.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class TableRekeyingTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic1 = "outputTopic1";
  private static final String outputTopic2 = "outputTopic2";
  private static final String outputTopic3 = "outputTopic3";

  @Test
  public void shouldRekeyTheTable() {
    final List<KeyValue<Integer, String>> inputRecords = Arrays.asList(
        new KeyValue<>(123, "kafka"),
        new KeyValue<>(123, "stream"),
        new KeyValue<>(456, "stream"),
        new KeyValue<>(456, "table"),
        new KeyValue<>(456, "duality"),
        new KeyValue<>(789, "will be removed next"),
        new KeyValue<>(789, null)
    );

    final Map<String, Integer> expectedOutput = new HashMap<>();
    expectedOutput.put("kafka", 123);
    expectedOutput.put("stream", 456); // Note how, for key "stream", the value is 456 and not 123.
    expectedOutput.put("table", 456);
    expectedOutput.put("duality", 456);
    // Note how there isn't any entry for the original key 789.

    // Step 1: Create the topology and its configuration
    final StreamsBuilder builder = createTopology();
    final Properties streamsConfiguration = createTopologyConfiguration();

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      // Step 2: Write the input
      IntegrationTestUtils.produceKeyValuesSynchronously(
          inputTopic, inputRecords, topologyTestDriver, new IntegerSerializer(), new StringSerializer());

      // Step 3: Validate the output

      // Variant 1
      final Map<String, Integer> actualOutput1 = IntegrationTestUtils.drainTableOutput(
          outputTopic1, topologyTestDriver, new StringDeserializer(), new IntegerDeserializer());
      assertThat(actualOutput1).isEqualTo(expectedOutput);

      // Variant 2
      final Map<String, Integer> actualOutput2 = IntegrationTestUtils.drainTableOutput(
          outputTopic2, topologyTestDriver, new StringDeserializer(), new IntegerDeserializer());
      assertThat(actualOutput2).isEqualTo(expectedOutput);

      // Variant 3
      final Map<String, Integer> actualOutput3 = IntegrationTestUtils.drainTableOutput(
          outputTopic3, topologyTestDriver, new StringDeserializer(), new IntegerDeserializer());
      assertThat(actualOutput3).isEqualTo(expectedOutput);
    }
  }

  private StreamsBuilder createTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    final KTable<Integer, String> table = builder.table(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()));

    // Variant 1
    final KTable<String, Integer> rekeyedTable1 = table
        .groupBy(
            (key, value) -> KeyValue.pair(value, key),
            Grouped.with(Serdes.String(), Serdes.Integer()))
        .reduce(
            (aggValue, newValue) -> newValue, /* adder */
            (aggValue, oldValue) -> oldValue  /* subtractor */);
    rekeyedTable1.toStream().to(outputTopic1, Produced.with(Serdes.String(), Serdes.Integer()));

    // Variant 2
    final KTable<String, Integer> rekeyedTable2 = table
        .groupBy(
            (key, value) -> KeyValue.pair(value, key),
            Grouped.with(Serdes.String(), Serdes.Integer()))
        .aggregate(
            () -> 0,
            (aggKey, newValue, aggValue) -> newValue, /* adder */
            (aggKey, oldValue, aggValue) -> oldValue, /* subtractor */
            Materialized.with(Serdes.String(), Serdes.Integer())
        );
    rekeyedTable2.toStream().to(outputTopic2, Produced.with(Serdes.String(), Serdes.Integer()));

    // Variant 3
    final KTable<String, Integer> rekeyedTable3 =
        table
            .toStream()
            .map((key, value) -> KeyValue.pair(value, key))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            // Dummy aggregation
            .reduce((aggValue, newValue) -> newValue);
    rekeyedTable3.toStream().to(outputTopic3, Produced.with(Serdes.String(), Serdes.Integer()));

    return builder;
  }

  private Properties createTopologyConfiguration() {
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
    return streamsConfiguration;
  }

}