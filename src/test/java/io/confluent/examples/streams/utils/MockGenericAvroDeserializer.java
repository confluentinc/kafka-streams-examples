package io.confluent.examples.streams.utils;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class MockGenericAvroDeserializer implements Deserializer<GenericRecord> {
  private KafkaAvroDeserializer inner;
  private final boolean constructedWithClient;

  MockGenericAvroDeserializer() {
    constructedWithClient = false;
  }

  MockGenericAvroDeserializer(final MockSchemaRegistryClient mockSchemaRegistryClient) {
    inner = new KafkaAvroDeserializer(mockSchemaRegistryClient);
    constructedWithClient = true;
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    if (!constructedWithClient) {
      final MockSchemaRegistryClient mockSchemaRegistryClient =
        (MockSchemaRegistryClient) configs.get("mock.schema.registry.client");
      inner = new KafkaAvroDeserializer(mockSchemaRegistryClient);
    }
    inner.configure(configs, isKey);
  }

  @Override
  public GenericRecord deserialize(final String topic, final byte[] data) {
    return (GenericRecord) inner.deserialize(topic, data);
  }

  @Override
  public void close() {
    inner.close();
  }
}
