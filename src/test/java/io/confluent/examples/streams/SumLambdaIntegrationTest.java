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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that shows how to compute the sum of numbers, based on {@link
 * SumLambdaExample}.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class SumLambdaIntegrationTest {

  private static String inputTopic = "inputTopic";
  private static String outputTopic = "outputTopic";

  @Test
  public void shouldSumEvenNumbers() throws Exception {
    final List<Integer> inputValues = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    final List<Integer> expectedValues = Collections.singletonList(30);

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final KStream<Void, Integer> input = builder.stream(inputTopic);
    final KTable<Integer, Integer> sumOfOddNumbers = input
        .filter((k, v) -> v % 2 == 0)
        .selectKey((k, v) -> 1)
        // no need to specify explicit serdes because the resulting key and value types match our default serde settings
        .groupByKey()
        .reduce((v1, v2) -> v1 + v2);
    sumOfOddNumbers.toStream().to(outputTopic);

    final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration);

    //
    // Step 2: Produce some input data to the input topic.
    //
    IntegrationTestUtils.produceKeyValuesSynchronously(
      inputTopic,
      inputValues.stream().map(i -> new KeyValue<Void, Integer>(null, i)).collect(Collectors.toList()),
      topologyTestDriver,
      new IntegrationTestUtils.NothingSerde<>(true),
      new IntegerSerializer()
    );

    //
    // Step 3: Verify the application's output data.
    //
    final Collection<Integer> actualValues = IntegrationTestUtils.drainTableOutput(
      outputTopic,
      topologyTestDriver,
      new IntegerDeserializer(),
      new IntegerDeserializer()
    ).values();
    assertThat(actualValues).containsExactlyElementsOf(expectedValues);
  }

}
