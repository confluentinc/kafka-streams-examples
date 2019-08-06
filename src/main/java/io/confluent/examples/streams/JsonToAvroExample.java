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

import io.confluent.examples.streams.avro.WikiFeed;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * A simple example demonstrating how to convert records in JSON format in a given topic to records
 * serialized in Avro format.
 * <p> Note: The specific Avro binding is used for serialization/deserialization, where the {@code
 * WikiFeed} class is auto-generated from its Avro schema by the maven avro plugin. See {@code
 * wikifeed.avsc} under {@code src/main/resources/avro/io/confluent/examples/streams/}. <p> <br> HOW
 * TO RUN THIS EXAMPLE <p> 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer
 * to
 * <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>. <p> 2)
 * Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic json-source \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic avro-sink \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code
 * bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-5.4.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.JsonToAvroExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link JsonToAvroExampleDriver}). The
 * already running example application (step 3) will automatically process this input data and write
 * the results to the output topic. The {@link JsonToAvroExampleDriver} will print the results from
 * the output topic
 * <pre>
 * {@code
 * # Here: Write input data using the example driver.  Once the driver has stopped generating data,
 * # you can terminate it via Ctrl-C.
 * $ java -cp target/kafka-streams-examples-5.4.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.JsonToAvroExampleDriver
 * }
 * </pre>
 */
public class JsonToAvroExample {

  static final String JSON_SOURCE_TOPIC = "json-source";
  static final String AVRO_SINK_TOPIC = "avro-sink";

  public static void main(final String[] args) {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    final KafkaStreams streams = buildJsonToAvroStream(
        bootstrapServers,
        schemaRegistryUrl
    );
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  static KafkaStreams buildJsonToAvroStream(final String bootstrapServers,
                                            final String schemaRegistryUrl) {
    final Properties streamsConfiguration = new Properties();

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "json-to-avro-stream-conversion");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "json-to-avro-stream-conversion-client");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Where to find the Confluent schema registry instance(s)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100 * 1000);

    final ObjectMapper objectMapper = new ObjectMapper();

    final StreamsBuilder builder = new StreamsBuilder();

    // read the source stream
    final KStream<String, String> jsonToAvroStream = builder.stream(JSON_SOURCE_TOPIC,
                                                                    Consumed.with(Serdes.String(), Serdes.String()));
    jsonToAvroStream.mapValues(v -> {
      WikiFeed wikiFeed = null;
      try {
        final JsonNode jsonNode = objectMapper.readTree(v);
        wikiFeed = new WikiFeed(jsonNode.get("user").asText(),
                                jsonNode.get("is_new").asBoolean(),
                                jsonNode.get("content").asText());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return wikiFeed;
    }).filter((k,v) -> v != null).to(AVRO_SINK_TOPIC);

    return new KafkaStreams(builder.build(), streamsConfiguration);
  }

}
