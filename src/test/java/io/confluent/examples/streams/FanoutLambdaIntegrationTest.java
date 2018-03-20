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

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates "fan-out", using an embedded Kafka cluster.
 *
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
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class FanoutLambdaIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static String inputTopicA = "A";
  private static String outputTopicB = "B";
  private static String outputTopicC = "C";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(inputTopicA);
    CLUSTER.createTopic(outputTopicB);
    CLUSTER.createTopic(outputTopicC);
  }

  @Test
  public void shouldFanoutTheInput() throws Exception {
    List<String> inputValues = Arrays.asList("Hello", "World");
    List<String> expectedValuesForB = inputValues.stream().map(String::toUpperCase).collect(Collectors.toList());
    List<String> expectedValuesForC = inputValues.stream().map(String::toLowerCase).collect(Collectors.toList());

    //
    // Step 1: Configure and start the processor topology.
    //
    StreamsBuilder builder = new StreamsBuilder();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "fanout-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KStream<byte[], String> stream1 = builder.stream(inputTopicA);
    KStream<byte[], String> stream2 = stream1.mapValues(String::toUpperCase);
    KStream<byte[], String> stream3 = stream1.mapValues(String::toLowerCase);
    stream2.to(outputTopicB);
    stream3.to(outputTopicC);

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    //
    // Step 2: Produce some input data to the input topic.
    //
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils.produceValuesSynchronously(inputTopicA, inputValues, producerConfig);

    //
    // Step 3: Verify the application's output data.
    //

    // Verify output topic B
    Properties consumerConfigB = new Properties();
    consumerConfigB.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfigB.put(ConsumerConfig.GROUP_ID_CONFIG, "fanout-lambda-integration-test-standard-consumer-topicB");
    consumerConfigB.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfigB.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfigB.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<String> actualValuesForB = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfigB,
        outputTopicB, inputValues.size());
    assertThat(actualValuesForB).isEqualTo(expectedValuesForB);

    // Verify output topic C
    Properties consumerConfigC = new Properties();
    consumerConfigC.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfigC.put(ConsumerConfig.GROUP_ID_CONFIG, "fanout-lambda-integration-test-standard-consumer-topicC");
    consumerConfigC.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfigC.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfigC.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<String> actualValuesForC = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfigC,
        outputTopicC, inputValues.size());
    streams.close();
    assertThat(actualValuesForC).isEqualTo(expectedValuesForC);
  }

}