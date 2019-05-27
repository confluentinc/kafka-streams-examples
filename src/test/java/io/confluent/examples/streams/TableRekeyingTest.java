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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.examples.streams.utils.ListSerde;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How to re-key (re-partition) a KTable, resulting in a new KTable.
 * This specific example flips keys and values in the original table.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class TableRekeyingTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

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
    expectedOutput.put("stream", 123);
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
      final Map<String, Integer> actualOutput = IntegrationTestUtils.drainTableOutput(
          outputTopic, topologyTestDriver, new StringDeserializer(), new IntegerDeserializer());
      assertThat(actualOutput).isEqualTo(expectedOutput);
    }
  }

  private StreamsBuilder createTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    final KTable<Integer, String> table = builder.table(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()));

    final KTable<String, Integer> rekeyedTable = table
        .groupBy(
            (key, value) -> KeyValue.pair(value, key),
            Grouped.with(Serdes.String(), Serdes.Integer()))
        .aggregate(
            LinkedList::new,
            /* adder */
            (aggKey, newValue, aggValue) -> {
              aggValue.add(newValue);
              return aggValue;
            },
            /* subtractor */
            (aggKey, newValue, aggValue) -> {
              aggValue.remove(aggValue.size() - 1);
              return aggValue;
            },
            Materialized.with(Serdes.String(), new ListSerde<>(Serdes.Integer()))
        )
        .mapValues(xs -> xs.size() > 0 ? xs.get(xs.size() - 1) : null);
    rekeyedTable.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));

    return builder;
  }

  private Properties createTopologyConfiguration() {
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-rekeying-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
    return streamsConfiguration;
  }

}