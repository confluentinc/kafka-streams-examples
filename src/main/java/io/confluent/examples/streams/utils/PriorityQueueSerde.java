package io.confluent.examples.streams.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

public class PriorityQueueSerde<T> implements Serde<PriorityQueue<T>> {

  private final Serde<PriorityQueue<T>> inner;

  public PriorityQueueSerde(final Comparator<T> comparator, final Serde<T> avroSerde) {
    inner = Serdes.serdeFrom(new PriorityQueueSerializer<>(comparator, avroSerde.serializer()),
                             new PriorityQueueDeserializer<>(comparator, avroSerde.deserializer()));
  }

  @Override
  public Serializer<PriorityQueue<T>> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<PriorityQueue<T>> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

}