package io.confluent.examples.streams.utils;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MockGenericAvroSerde implements Serde<GenericRecord> {
  private final MockGenericAvroSerializer serializer;
  private final MockGenericAvroDeserializer deserializer;

  public MockGenericAvroSerde() {
    serializer = new MockGenericAvroSerializer();
    deserializer = new MockGenericAvroDeserializer();
  }

  public MockGenericAvroSerde(final MockSchemaRegistryClient mockSchemaRegistryClient) {
    serializer = new MockGenericAvroSerializer(mockSchemaRegistryClient);
    deserializer = new MockGenericAvroDeserializer(mockSchemaRegistryClient);
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    serializer.configure(configs, isKey);
    deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    serializer.close();
    deserializer.close();
  }

  @Override
  public Serializer<GenericRecord> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<GenericRecord> deserializer() {
    return deserializer;
  }
}
