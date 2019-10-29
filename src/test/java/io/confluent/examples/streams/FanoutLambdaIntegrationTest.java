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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates "fan-out", using an embedded Kafka cluster.
 * <p>
 * This example shows how you can read from one input topic/stream, transform the data (here:
 * trivially) in two different ways via two intermediate streams, and then write the respective
 * results to two output topics.
 *
 * <pre>
 * {@code
 *
 *                                         +---map()---> stream2 ---to()---> Kafka topic B
 *                                         |
 * Kafka topic A ---stream()--> stream1 ---+
 *                                         |
 *                                         +---map()---> stream3 ---to()---> Kafka topic C
 *
 * }
 * </pre>
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class FanoutLambdaIntegrationTest {

  @Test
  public void shouldFanoutTheInput() {
    final List<String> inputValues = Arrays.asList("Hello", "World");
    final List<String> expectedValuesForB = inputValues.stream().map(String::toUpperCase).collect(Collectors.toList());
    final List<String> expectedValuesForC = inputValues.stream().map(String::toLowerCase).collect(Collectors.toList());

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "fanout-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    final String inputTopicA = "A";
    final String outputTopicB = "B";
    final String outputTopicC = "C";
    final KStream<byte[], String> stream1 = builder.stream(inputTopicA);
    final KStream<byte[], String> stream2 = stream1.mapValues(s -> s.toUpperCase());
    final KStream<byte[], String> stream3 = stream1.mapValues(s -> s.toLowerCase());
    stream2.to(outputTopicB);
    stream3.to(outputTopicC);

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<Void, String> input = topologyTestDriver
        .createInputTopic(inputTopicA,
                          new IntegrationTestUtils.NothingSerde<>(),
                          new StringSerializer());
      final TestOutputTopic<Void, String> outputB = topologyTestDriver
        .createOutputTopic(outputTopicB,
                           new IntegrationTestUtils.NothingSerde<>(),
                           new StringDeserializer());
      final TestOutputTopic<Void, String> outputC = topologyTestDriver
        .createOutputTopic(outputTopicC,
                           new IntegrationTestUtils.NothingSerde<>(),
                           new StringDeserializer());

      //
      // Step 3: Produce some input data to the input topic.
      //
      input.pipeValueList(inputValues);

      //
      // Step 4: Verify the application's output data.
      //

      // Verify output topic B
      assertThat(outputB.readValuesToList(), equalTo(expectedValuesForB));
      // Verify output topic C
      assertThat(outputC.readValuesToList(), equalTo(expectedValuesForC));
    }
  }
}