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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates how to handle corrupt input records (think: poison
 * pill messages) in a Kafka topic, which would normally lead to application failures due to
 * (de)serialization exceptions.
 * <p>
 * In this example we choose to ignore/skip corrupted input records.  We describe further options at
 * http://docs.confluent.io/current/streams/faq.html, e.g. sending corrupted records to a quarantine
 * topic (think: dead letter queue).
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class HandlingCorruptedInputRecordsIntegrationTest {

  @Test
  public void shouldIgnoreCorruptInputRecords() {
    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "failure-handling-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final String inputTopic = "inputTopic";
    final String outputTopic = "outputTopic";

    final KStream<byte[], byte[]> stream = builder.stream(inputTopic);

    // Note how the returned stream is of type `KStream<String, Long>`.
    final KStream<String, Long> doubled = stream.flatMap(
      (k, v) -> {
        try {
          // Attempt deserialization
          final String key = stringSerde.deserializer().deserialize("input-topic", k);
          final long value = longSerde.deserializer().deserialize("input-topic", v);

          // Ok, the record is valid (not corrupted).  Let's take the
          // opportunity to also process the record in some way so that
          // we haven't paid the deserialization cost just for "poison pill"
          // checking.
          return Collections.singletonList(KeyValue.pair(key, 2 * value));
        } catch (final SerializationException e) {
          // Ignore/skip the corrupted record by catching the exception.
          // Optionally, we can log the fact that we did so:
          System.err.println("Could not deserialize record: " + e.getMessage());
        }
        return Collections.emptyList();
      }
    );

    // Write the processing results (which was generated from valid records only) to Kafka.
    doubled.to(outputTopic, Produced.with(stringSerde, longSerde));


    final List<Long> inputValues = Arrays.asList(1L, 2L, 3L);
    final LongSerializer longSerializer = new LongSerializer();
    final List<byte[]> serializedInputValues =
        inputValues.stream().map(x -> longSerializer.serialize(inputTopic, x)).collect(Collectors.toList());
    final List<Long> expectedValues = inputValues.stream().map(x -> 2 * x).collect(Collectors.toList());

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      // setup input topic as byte[]-value type to allow ingesting corrupted data
      final TestInputTopic<Void, byte[]> input = topologyTestDriver
        .createInputTopic(inputTopic,
                          new IntegrationTestUtils.NothingSerde<>(),
                          new ByteArraySerializer());
      final TestOutputTopic<Void, Long> output = topologyTestDriver
        .createOutputTopic(outputTopic,
                           new IntegrationTestUtils.NothingSerde<>(),
                           new LongDeserializer());

      //
      // Step 3: Produce some corrupt input data to the input topic.
      //
      input.pipeInput(new StringSerializer().serialize(inputTopic, "corrupt"));

      //
      // Step 4: Produce some (valid) input data to the input topic.
      //
      input.pipeValueList(serializedInputValues);

      //
      // Step 5: Verify the application's output data.
      //
      assertThat(output.readValuesToList(), equalTo(expectedValues));
    }
  }
}
