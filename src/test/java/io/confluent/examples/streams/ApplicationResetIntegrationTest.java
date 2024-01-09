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
import kafka.tools.StreamsResetter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ApplicationResetIntegrationTest {
  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String applicationId = "application-reset-integration-test";
  private static final String inputTopic = "my-input-topic";
  private static final String outputTopic = "my-output-topic";

  @BeforeClass
  public static void startKafkaCluster() throws InterruptedException {
    CLUSTER.createTopic(inputTopic);
    CLUSTER.createTopic(outputTopic);
  }

  @Test
  public void shouldReprocess() throws Exception {
    final List<String> inputValues = Arrays.asList("Hello World", "Hello Kafka Streams", "All streams lead to Kafka");
    final List<KeyValue<String, Long>> expectedResult = Arrays.asList(
      KeyValue.pair("Hello", 1L),
      KeyValue.pair("Hello", 2L),
      KeyValue.pair("All", 1L)
    );

    final Properties verificationConsumerConfig = new Properties();
    verificationConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    verificationConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "application-reset-integration-test-standard-consumer-output-topic");
    verificationConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    verificationConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    verificationConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

    //
    // Step 1: Configure and start the processor topology.
    //
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    streamsConfiguration.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

    {
      final KafkaStreams streams = ApplicationResetExample.buildKafkaStreams(streamsConfiguration);

      ApplicationResetExample.startKafkaStreamsSynchronously(streams);

      //
      // Step 2: Produce some input data to the input topic.
      //
      final Properties producerConfig = new Properties();
      producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
      producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
      producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
      producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);

      //
      // Step 3: Verify the application's output data.
      //


      final List<KeyValue<String, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
        verificationConsumerConfig,
        outputTopic,
        inputValues.size()
      );
      assertThat(result, equalTo(expectedResult));

      streams.close();
    }


    //
    // Step 4: Reset application.
    //

    // wait for application to be completely shut down
    final Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    final AdminClient adminClient = AdminClient.create(config);
    while (!adminClient.describeConsumerGroups(Collections.singleton(applicationId)).all().get().get(applicationId).members().isEmpty()) {
      Utils.sleep(50);
    }

    // reset application
    final int exitCode = new StreamsResetter().run(
      new String[]{
        "--application-id", applicationId,
        "--bootstrap-servers", CLUSTER.bootstrapServers(),
        "--zookeeper", CLUSTER.zookeeperConnect(),
        "--input-topics", inputTopic
      });
    Assert.assertEquals(0, exitCode);

    // wait for reset client to be completely closed
    while (!adminClient.describeConsumerGroups(Collections.singleton(applicationId)).all().get().get(applicationId).members().isEmpty()) {
      Utils.sleep(50);
    }

    {
      //
      // Step 5: Rerun application
      //
      final KafkaStreams streams = ApplicationResetExample.buildKafkaStreams(streamsConfiguration);

      // Delete the application's local state
      streams.cleanUp();
      ApplicationResetExample.startKafkaStreamsSynchronously(streams);

      //
      // Step 6: Verify the application's output data.
      //
      final List<KeyValue<String, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
        verificationConsumerConfig,
        outputTopic,
        inputValues.size()
      );
      assertThat(resultRerun, equalTo(expectedResult));

      streams.close();
    }
  }

}
