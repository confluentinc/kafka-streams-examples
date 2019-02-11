package io.confluent.examples.streams.utils;

import org.apache.kafka.streams.KeyValue;

public class KeyValueWithTimestamp<K, V> extends KeyValue<K, V> {

  /**
   * Timestamp of Kafka message (milliseconds since the epoch).
   */
  public final long timestamp;

  public KeyValueWithTimestamp(final K key, final V value, final long timestamp) {
    super(key, value);
    this.timestamp = timestamp;
  }

  public KeyValueWithTimestamp(final K key, final V value) {
    this(key, value, System.currentTimeMillis());
  }

}