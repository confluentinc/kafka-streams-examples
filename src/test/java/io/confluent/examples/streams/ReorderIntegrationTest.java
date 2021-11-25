/*
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates how to reorder the stream of incoming messages
 * by the timestamp embedded in the message payload.
 * <p>
 * Makes sense only on per partition basis.
 * <p>
 * Reordering occurs within the time windows defined by the {@code grace} constructor parameter.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class ReorderIntegrationTest {

  public static class ReorderTransformer<K, V>
      implements Transformer<K, V, KeyValue<K, V>> {

    public interface StoreKeyGenerator<K, V> {
      K getStoreKey(K key, V val);
    }

    private final String storeName;
    private final Duration grace;
    private KeyValueStore<K, V> reorderStore;
    private ProcessorContext context;
    private final StoreKeyGenerator<K,V> storeKeyGenerator;

    public ReorderTransformer(final String storeName, final Duration grace,
                              final StoreKeyGenerator<K,V> storeKeyGenerator) {
      this.storeName = storeName;
      this.grace = grace;
      this.storeKeyGenerator = storeKeyGenerator;
    }

    @Override
    public void init(final ProcessorContext context) {
      this.reorderStore = context.getStateStore(this.storeName);
      this.context = context;
      context.schedule(
          this.grace,
          PunctuationType.STREAM_TIME,
          this::punctuate
      );
    }

    /**
     * 1) read the timestamp from the message value
     * 2) inserts into a KeyValueStore using (timestamp, message-key) pair
     * as the key and the message-value as the value, this also provides
     * de-duplication.
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    public KeyValue<K, V> transform(final K key, final V value) {
      // Keys need to contain and be sortable by time
      final K storeKey = storeKeyGenerator.getStoreKey(key, value);
      final V storeValue = reorderStore.get(storeKey);
      if(storeValue == null) {
        reorderStore.put(storeKey, value);
      }
      return null; 	// null suppresses sending to downstream
    }

    /**
     * Scheduled to be called automatically when the period
     * within which message reordering occurs expires.
     *
     * Outputs downstream accumulated records sorted by their timestamp.
     *
     * 1) read the entire store
     * 2) send the fetched messages in order using context.forward() and delete
     * them from the store
     *
     * @param timestamp – stream time of the punctuate function call
     */
    void punctuate(final long timestamp) {
      try(KeyValueIterator<K, V> it = reorderStore.all()) {
        while (it.hasNext()) {
          final KeyValue<K, V> kv = it.next();
          context.forward(kv.key, kv.value);
          reorderStore.delete(kv.key);
        }
      }
    }

    @Override
    public void close() {
    }
  }


  public static class TestTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
      return (long) record.value();
    }
  }

  private static long ts(final String timeString) throws ParseException {
    return Instant.parse(timeString).toEpochMilli();
  }

  @Test
  public void shouldReorderTheInput() throws ParseException {

    //  Input not ordered by time
    final List<Long> inputValues = Arrays.asList(
        ts("2021-11-03T23:00:00Z"),    // stream time calibration
        ts("2021-11-04T01:05:00Z"),    // 10 hours interval border is at "2021-11-04T00:00:00Z"
        ts("2021-11-04T01:10:00Z"),
        ts("2021-11-04T01:40:00Z"),
        ts("2021-11-04T02:25:00Z"),
        ts("2021-11-04T01:20:00Z"),
        ts("2021-11-04T02:45:00Z"),
        ts("2021-11-04T02:00:00Z"),
        ts("2021-11-04T03:00:00Z"),
        ts("2021-11-04T02:40:00Z"),
        ts("2021-11-04T02:20:00Z"),    // 10 hours interval border is at "2021-11-04T10:00:00Z"
        ts("2021-11-05T00:00:00Z")     // stream time calibration
    );

    //  Expected ordered by time
    final List<Long> expectedValues = Arrays.asList(
        ts("2021-11-03T23:00:00Z"),    // stream time calibration
        ts("2021-11-04T01:05:00Z"),
        ts("2021-11-04T01:10:00Z"),
        ts("2021-11-04T01:20:00Z"),
        ts("2021-11-04T01:40:00Z"),
        ts("2021-11-04T02:00:00Z"),
        ts("2021-11-04T02:20:00Z"),
        ts("2021-11-04T02:25:00Z"),
        ts("2021-11-04T02:40:00Z"),
        ts("2021-11-04T02:45:00Z"),
        ts("2021-11-04T03:00:00Z"),
        ts("2021-11-05T00:00:00Z")     // stream time calibration
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "reorder-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TestTimestampExtractor.class.getName());

    final String inputTopic = "inputTopic";
    final String outputTopic = "outputTopic";
    final String persistentStore = "reorderStore";
    final String transformerName = "reorderTransformer";

    final StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(persistentStore),
            Serdes.String(),
            Serdes.Long());

    final KStream<String, Long> stream = builder.stream(inputTopic);
    final KStream<String, Long> reordered = stream
      .transform(() -> new ReorderTransformer<>(persistentStore,
          Duration.of(10, HOURS), (k,v) -> String.format("key-%d", v)),
          Named.as(transformerName));
    reordered.to(outputTopic);

    final Topology topology = builder.build();
    topology.addStateStore(countStoreSupplier, transformerName);

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<String, Long> input = topologyTestDriver
        .createInputTopic(inputTopic,
                          new Serdes.StringSerde().serializer(),
                          new LongSerializer());
      final TestOutputTopic<String, Long> output = topologyTestDriver
        .createOutputTopic(outputTopic,
                          new Serdes.StringSerde().deserializer(),
                          new LongDeserializer());

      //
      // Step 3: Produce some input data to the input topic.
      //
      input.pipeValueList(inputValues);

      //
      // Step 4: Verify the application's output data.
      //
      assertThat(output.readValuesToList(), equalTo(expectedValues));
    }
  }
}