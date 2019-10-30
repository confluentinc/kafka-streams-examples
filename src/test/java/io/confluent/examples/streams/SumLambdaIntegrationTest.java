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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that shows how to compute the sum of numbers, based on {@link
 * SumLambdaExample}.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class SumLambdaIntegrationTest {

  @Test
  public void shouldSumEvenNumbers() {
    final List<Integer> inputValues = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    final List<KeyValue<Integer, Integer>> expectedValues = Arrays.asList(
      new KeyValue<>(1, 1),
      new KeyValue<>(1, 4),
      new KeyValue<>(1, 9),
      new KeyValue<>(1, 16),
      new KeyValue<>(1, 25)
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());


    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(SumLambdaExample.getTopology(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<Void, Integer> input = topologyTestDriver
        .createInputTopic(SumLambdaExample.NUMBERS_TOPIC,
                          new IntegrationTestUtils.NothingSerde<>(),
                          new IntegerSerializer());
      final TestOutputTopic<Integer, Integer> output = topologyTestDriver
        .createOutputTopic(SumLambdaExample.SUM_OF_ODD_NUMBERS_TOPIC,
                           new IntegerDeserializer(),
                           new IntegerDeserializer());

      //
      // Step 3: Produce some input data to the input topic.
      //
      input.pipeValueList(inputValues);

      //
      // Step 4: Verify the application's output data.
      //
      assertThat(output.readKeyValuesToList(), equalTo(expectedValues));
    }
  }

}
