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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates how to work on Generic Avro data.
 * <p>
 * See {@link SpecificAvroIntegrationTest} for the equivalent Specific Avro integration test.
 */
public class GenericAvroIntegrationTest {

  // A mocked schema registry for our serdes to use
  private static final String SCHEMA_REGISTRY_SCOPE = GenericAvroIntegrationTest.class.getName();
  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;


  private static String inputTopic = "inputTopic";
  private static String outputTopic = "outputTopic";

  @Test
  public void shouldRoundTripGenericAvroDataThroughKafka() throws Exception {

    final Schema schema = new Schema.Parser().parse(
      getClass().getResourceAsStream("/avro/io/confluent/examples/streams/wikifeed.avsc")
    );

    final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);

    schemaRegistryClient.register("inputTopic-value", schema);

    final GenericRecord record = new GenericData.Record(schema);
    record.put("user", "alice");
    record.put("is_new", true);
    record.put("content", "lorem ipsum");
    final List<Object> inputValues = Collections.singletonList(record);

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "generic-avro-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

    // Write the input data as-is to the output topic.
    //
    // Normally, because a) we have already configured the correct default serdes for keys and
    // values and b) the types for keys and values are the same for both the input topic and the
    // output topic, we would only need to define:
    //
    //   builder.stream(inputTopic).to(outputTopic);
    //
    // However, in the code below we intentionally override the default serdes in `to()` to
    // demonstrate how you can construct and configure a generic Avro serde manually.
    final Serde<String> stringSerde = Serdes.String();
    final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
    // Note how we must manually call `configure()` on this serde to configure the schema registry.
    // This is different from the case of setting default serdes (see `streamsConfiguration`
    // above), which will be auto-configured based on the `StreamsConfiguration` instance.
    genericAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL),
      /*isKey*/ false);
    final KStream<String, GenericRecord> stream = builder.stream(inputTopic);
    stream.to(outputTopic, Produced.with(stringSerde, genericAvroSerde));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)){
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<Void, Object> input = topologyTestDriver
        .createInputTopic(inputTopic,
                          new IntegrationTestUtils.NothingSerde<>(),
                          new KafkaAvroSerializer(schemaRegistryClient));
      final TestOutputTopic<Void, Object> output = topologyTestDriver
        .createOutputTopic(outputTopic,
                           new IntegrationTestUtils.NothingSerde<>(),
                           new KafkaAvroDeserializer(schemaRegistryClient));

      //
      // Step 3: Produce some input data to the input topic.
      //
      input.pipeValueList(inputValues);

      //
      // Step 4: Verify the application's output data.
      //
      assertThat(output.readValuesToList(), equalTo(inputValues));
    } finally {
      MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }
  }
}