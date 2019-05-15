package io.confluent.examples.streams.utils;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MockGenericAvroSerializer implements Serializer<GenericRecord> {
  private KafkaAvroSerializer inner;
  private final boolean constructedWithClient;

  MockGenericAvroSerializer() {
    constructedWithClient = false;
  }

  MockGenericAvroSerializer(final MockSchemaRegistryClient mockSchemaRegistryClient) {
    inner = new KafkaAvroSerializer(mockSchemaRegistryClient);
    constructedWithClient = true;
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    if (!constructedWithClient) {
      final MockSchemaRegistryClient mockSchemaRegistryClient =
        (MockSchemaRegistryClient) configs.get("mock.schema.registry.client");
      inner = new KafkaAvroSerializer(mockSchemaRegistryClient);
    }
    inner.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(final String topic, final GenericRecord data) {
    return inner.serialize(topic, data);
  }

  @Override
  public void close() {
    inner.close();
  }
}
