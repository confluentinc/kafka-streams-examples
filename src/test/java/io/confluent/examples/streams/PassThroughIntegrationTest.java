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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that reads data from an input topic and writes the same data as-is to
 * a new output topic, using an embedded Kafka cluster.
 */
public class PassThroughIntegrationTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @Test
  public void shouldWriteTheInputDataAsIsToTheOutputTopic() {
    final List<String> inputValues = Arrays.asList(
      "hello world",
      "the world is not enough",
      "the world of the stock market is coming to an end"
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pass-through-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    // Write the input data as-is to the output topic.
    builder.stream(inputTopic).to(outputTopic);

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Produce some input data to the input topic.
      //
      IntegrationTestUtils.produceKeyValuesSynchronously(
        inputTopic,
        inputValues.stream().map(v -> new KeyValue<>(null, v)).collect(Collectors.toList()),
        topologyTestDriver,
        new IntegrationTestUtils.NothingSerde<>(),
        new StringSerializer()
      );

      //
      // Step 3: Verify the application's output data.
      //
      final List<String> actualValues =
        IntegrationTestUtils.drainStreamOutput(
          outputTopic,
          topologyTestDriver,
          new IntegrationTestUtils.NothingSerde<>(),
          new StringDeserializer()
        )
          .stream()
          .map(kv -> kv.value)
          .collect(Collectors.toList());

      assertThat(actualValues).isEqualTo(inputValues);
    }
  }

}
