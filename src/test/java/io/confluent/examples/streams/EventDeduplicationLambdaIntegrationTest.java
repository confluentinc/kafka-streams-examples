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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates how to remove duplicate records from an input
 * stream.
 * <p>
 * Here, a stateful {@link org.apache.kafka.streams.kstream.Transformer} (from the Processor API)
 * detects and discards duplicate input records based on an "event id" that is embedded in each
 * input record.  This transformer is then included in a topology defined via the DSL.
 * <p>
 * In this simplified example, the values of input records represent the event ID by which
 * duplicates will be detected.  In practice, record values would typically be a more complex data
 * structure, with perhaps one of the fields being such an event ID.  De-duplication by an event ID
 * is but one example of how to perform de-duplication in general.  The code example below can be
 * adapted to other de-duplication approaches.
 * <p>
 * IMPORTANT:  Kafka including its Streams API support exactly-once semantics since version 0.11.
 * With this feature available, most use cases will no longer need to worry about duplicate messages
 * or duplicate processing.  That said, there will still be some use cases where you have your own
 * business rules that define when two events are considered to be "the same" and need to be
 * de-duplicated (e.g. two events having the same payload but different timestamps).  The example
 * below demonstrates how to implement your own business rules for event de-duplication.
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class EventDeduplicationLambdaIntegrationTest {

  private static final String storeName = "eventId-store";

  /**
   * Discards duplicate records from the input stream.
   * <p>
   * Duplicate records are detected based on an event ID;  in this simplified example, the record
   * value is the event ID.  The transformer remembers known event IDs in an associated window state
   * store, which automatically purges/expires event IDs from the store after a certain amount of
   * time has passed to prevent the store from growing indefinitely.
   * <p>
   * Note: This code is for demonstration purposes and was not tested for production usage.
   */
  private static class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    private ProcessorContext context;

    /**
     * Key: event ID
     * Value: timestamp (event-time) of the corresponding event when the event ID was seen for the
     * first time
     */
    private WindowStore<E, Long> eventIdStore;

    private final long leftDurationMs;
    private final long rightDurationMs;

    private final KeyValueMapper<K, V, E> idExtractor;

    /**
     * @param maintainDurationPerEventInMs how long to "remember" a known event (or rather, an event
     *                                     ID), during the time of which any incoming duplicates of
     *                                     the event will be dropped, thereby de-duplicating the
     *                                     input.
     * @param idExtractor                  extracts a unique identifier from a record by which we de-duplicate input
     *                                     records; if it returns null, the record will not be considered for
     *                                     de-duping but forwarded as-is.
     */
    DeduplicationTransformer(final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
      if (maintainDurationPerEventInMs < 1) {
        throw new IllegalArgumentException("maintain duration per event must be >= 1");
      }
      leftDurationMs = maintainDurationPerEventInMs / 2;
      rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
      this.idExtractor = idExtractor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
      this.context = context;
      eventIdStore = (WindowStore<E, Long>) context.getStateStore(storeName);
    }

    public KeyValue<K, V> transform(final K key, final V value) {
      final E eventId = idExtractor.apply(key, value);
      if (eventId == null) {
        return KeyValue.pair(key, value);
      } else {
        final KeyValue<K, V> output;
        if (isDuplicate(eventId)) {
          output = null;
          updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
        } else {
          output = KeyValue.pair(key, value);
          rememberNewEvent(eventId, context.timestamp());
        }
        return output;
      }
    }

    private boolean isDuplicate(final E eventId) {
      final long eventTime = context.timestamp();
      final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
        eventId,
        eventTime - leftDurationMs,
        eventTime + rightDurationMs);
      final boolean isDuplicate = timeIterator.hasNext();
      timeIterator.close();
      return isDuplicate;
    }

    private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, final long newTimestamp) {
      eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }

    private void rememberNewEvent(final E eventId, final long timestamp) {
      eventIdStore.put(eventId, timestamp, timestamp);
    }

    @Override
    public void close() {
      // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
      // The Kafka Streams API will automatically close stores when necessary.
    }

  }

  @Test
  public void shouldRemoveDuplicatesFromTheInput() {
    final String firstId = UUID.randomUUID().toString(); // e.g. "4ff3cb44-abcb-46e3-8f9a-afb7cc74fbb8"
    final String secondId = UUID.randomUUID().toString();
    final String thirdId = UUID.randomUUID().toString();
    final List<String> inputValues = Arrays.asList(firstId, secondId, firstId, firstId, secondId, thirdId,
                                                   thirdId, firstId, secondId);
    final List<String> expectedValues = Arrays.asList(firstId, secondId, thirdId);

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplication-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    // How long we "remember" an event.  During this time, any incoming duplicates of the event
    // will be, well, dropped, thereby de-duplicating the input data.
    //
    // The actual value depends on your use case.  To reduce memory and disk usage, you could
    // decrease the size to purge old windows more frequently at the cost of potentially missing out
    // on de-duplicating late-arriving records.
    final Duration windowSize = Duration.ofMinutes(10);

    // retention period must be at least window size -- for this use case, we don't need a longer retention period
    // and thus just use the window size as retention time
    final Duration retentionPeriod = windowSize;

    final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(storeName,
                                         retentionPeriod,
                                         windowSize,
                                         false
            ),
            Serdes.String(),
            Serdes.Long());

    builder.addStateStore(dedupStoreBuilder);

    final String inputTopic = "inputTopic";
    final String outputTopic = "outputTopic";

    final KStream<byte[], String> stream = builder.stream(inputTopic);
    final KStream<byte[], String> deduplicated = stream.transform(
        // In this example, we assume that the record value as-is represents a unique event ID by
        // which we can perform de-duplication.  If your records are different, adapt the extractor
        // function as needed.
        () -> new DeduplicationTransformer<>(windowSize.toMillis(), (key, value) -> value),
        storeName);
    deduplicated.to(outputTopic);

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<Void, String> input = topologyTestDriver
        .createInputTopic(inputTopic,
                          new IntegrationTestUtils.NothingSerde<>(),
                          new StringSerializer());
      final TestOutputTopic<Void, String> output = topologyTestDriver
        .createOutputTopic(outputTopic,
                           new IntegrationTestUtils.NothingSerde<>(),
                           new StringDeserializer());

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