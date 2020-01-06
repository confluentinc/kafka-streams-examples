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
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This is a sample driver for the {@link JsonToAvroExample} and
 * To run this driver please first refer to the instructions in {@link JsonToAvroExample}
 * You can then run this class directly in your IDE or via the command line.
 * <p>
 * To run via the command line you might want to package as a fat-jar first. Please refer to:
 * <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>
 * <p>
 * Once packaged you can then run:
 * <pre>
 * {@code
 * java -cp target/kafka-streams-examples-5.4.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.JsonToAvroExampleDriver
 * }
 * </pre>
 *
 * You should terminate with {@code Ctrl-C}.
 */
public class JsonToAvroExampleDriver {

  public static void main(final String[] args) throws IOException {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    produceJsonInputs(bootstrapServers, schemaRegistryUrl);
    consumeAvroOutput(bootstrapServers, schemaRegistryUrl);
  }

  private static void produceJsonInputs(final String bootstrapServers, final String schemaRegistryUrl) throws IOException {
    final String[] users = {"Black Knight", "Sir Robin", "Knight Who Says Ni"};
    final ObjectMapper objectMapper = new ObjectMapper();

    final Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    try (final KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

      Arrays.stream(users).map(user -> {
        String jsonMap;
        try {
          final Map<String, String> map = new HashMap<>();
          map.put("user", user);
          map.put("is_new", "true");
          map.put("content", "flesh wound");
          jsonMap = objectMapper.writeValueAsString(map);
        } catch (final IOException e) {
          jsonMap = "{}";
        }
        return jsonMap;
      }).forEach(record -> producer
          .send(new ProducerRecord<>(JsonToAvroExample.JSON_SOURCE_TOPIC, null, record)));

      producer.flush();
    }
  }

  private static void consumeAvroOutput(final String bootstrapServers, final String schemaRegistryUrl) {
    final Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "json-to-avro-group");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class);
    consumerProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    final KafkaConsumer<String, WikiFeed> consumer = new KafkaConsumer<>(consumerProperties);

    consumer.subscribe(Collections.singleton(JsonToAvroExample.AVRO_SINK_TOPIC));
    while (true) {
      final ConsumerRecords<String, WikiFeed> consumerRecords = consumer.poll(Duration.ofSeconds(120));
      for (final ConsumerRecord<String, WikiFeed> consumerRecord : consumerRecords) {
        final WikiFeed wikiFeed = consumerRecord.value();
        System.out.println("Converted Avro Record " + wikiFeed.getUser() + " " + wikiFeed.getIsNew() + " " + wikiFeed.getContent());
      }
    }
  }
}
