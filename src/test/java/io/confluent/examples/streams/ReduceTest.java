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
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * How to aggregate messages via `groupByKey()` and `reduce()` to implement concatenation of data.
 *
 * This example can be adapted to structured (nested) data formats such as Avro or JSON in case you need to concatenate
 * only certain field(s) in the input.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 *
 * See {@link ReduceScalaTest} for the equivalent Scala example.
 */
public class ReduceTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @Test
  public void shouldConcatenateWithReduce() {
    final List<KeyValue<Integer, String>> inputRecords = Arrays.asList(
      new KeyValue<>(456, "stream"),
      new KeyValue<>(123, "hello"),
      new KeyValue<>(123, "world"),
      new KeyValue<>(456, "all"),
      new KeyValue<>(123, "kafka"),
      new KeyValue<>(456, "the"),
      new KeyValue<>(456, "things"),
      new KeyValue<>(123, "streams")
    );

    // For each record key, we want to concatenate the record values.
    final Map<Integer, String> expectedOutput = new HashMap<>();
    expectedOutput.put(456, "stream all the things");
    expectedOutput.put(123, "hello world kafka streams");

    //
    // Step 1: Create the topology and its configuration.
    //
    final StreamsBuilder builder = createTopology();
    final Properties streamsConfiguration = createTopologyConfiguration();

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<Integer, String> input = topologyTestDriver
        .createInputTopic(inputTopic, new IntegerSerializer(), new StringSerializer());
      final TestOutputTopic<Integer, String> output = topologyTestDriver
        .createOutputTopic(outputTopic, new IntegerDeserializer(), new StringDeserializer());

      //
      // Step 2: Write the input.
      //
      input.pipeKeyValueList(inputRecords);

      //
      // Step 3: Validate the output.
      //
      assertThat(output.readKeyValuesToMap(), equalTo(expectedOutput));
    }
  }

  private StreamsBuilder createTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<Integer, String> input = builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()));
    final KTable<Integer, String> concatenated =
      input
        // Group the records based on the existing key of records.
        .groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
        // For each key, concatenate the record values.
        .reduce((v1, v2) -> v1 + " " + v2);
    concatenated.toStream().to(outputTopic, Produced.with(Serdes.Integer(), Serdes.String()));
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