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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * How to aggregate messages via `groupBy()` and `aggregate()`.
 *
 * This example can be adapted to structured (nested) data formats such as Avro or JSON in case you need to aggregate
 * only certain field(s) in the input.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 *
 * See {@link AggregateScalaTest} for the equivalent Scala example.
 */
public class AggregateTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @Test
  public void shouldAggregate() {
    final List<String> inputValues = Arrays.asList(
      "stream", "all", "the", "things", "hi", "world", "kafka", "streams", "streaming"
    );

    // We want to compute the sum of the length of words (values) by the first letter (new key) of words.
    final Map<String, Long> expectedOutput = new HashMap<>();
    expectedOutput.put("a", 3L);
    expectedOutput.put("t", 9L);
    expectedOutput.put("h", 2L);
    expectedOutput.put("w", 5L);
    expectedOutput.put("k", 5L);
    expectedOutput.put("s", 22L);

    //
    // Step 1: Create the topology and its configuration.
    //
    final StreamsBuilder builder = createTopology();
    final Properties streamsConfiguration = createTopologyConfiguration();

    try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<Void, String> input = testDriver
       .createInputTopic(inputTopic,
                         new IntegrationTestUtils.NothingSerde<>(),
                         new StringSerializer());
      final TestOutputTopic<String, Long> output = testDriver
        .createOutputTopic(outputTopic, new StringDeserializer(), new LongDeserializer());

      //
      // Step 3: Write the input.
      //
      input.pipeValueList(inputValues);

      //
      // Step 4: Validate the output.
      //
      assertThat(output.readKeyValuesToMap(), equalTo(expectedOutput));
    }
  }

  private StreamsBuilder createTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<byte[], String> input = builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.String()));
    final KTable<String, Long> aggregated = input
      // We set the new key of a word (value) to the word's first letter.
      .groupBy(
        (key, value) -> (value != null && value.length() > 0) ? value.substring(0, 1).toLowerCase() : "",
        Grouped.with(Serdes.String(), Serdes.String()))
      // For each first letter (key), we compute the sum of the word lengths.
      .aggregate(
        () -> 0L,
        (aggKey, newValue, aggValue) -> aggValue + newValue.length(),
        Materialized.with(Serdes.String(), Serdes.Long())
      );
    aggregated.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
    return builder;
  }

  private Properties createTopologyConfiguration() {
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
    return streamsConfiguration;
  }
}