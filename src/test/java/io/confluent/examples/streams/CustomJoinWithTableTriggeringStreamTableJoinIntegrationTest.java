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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.utils.InstantSerde;
import io.confluent.examples.streams.utils.Pair;
import io.confluent.examples.streams.utils.PairOfDoubleAndLongDeserializer;
import io.confluent.examples.streams.utils.PairSerde;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates one way to implement a custom join operation.
 * Here, we implement custom Transformers with the Processor API and plug them into a DSL topology.
 *
 * Specifically, this example implements a stream-table join where both the stream side triggers a join output being
 * sent downstream (default behavior of KStreams) but also the table side (not supported yet in Kafka Streams out of the
 * box).  When a new record arrives on either side, a join output will be sent downstream immediately only if there is a
 * matching record on the other side of the join.  If there is no such match, no join output will be sent immediately.
 * Instead, the arriving record will be buffered to give the other side a certain (configurable) amount of time to,
 * hopefully, see a matching record to arrive eventually.  However, if the per-key wait time has exceeded, then an
 * "incomplete" join output will be sent downstream.
 *
 * The default stream-table join behavior of Kafka Streams (below: left join; inner join is similar) only triggers
 * join output when data arrives at the stream side.
 * See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics.
 *
 * Kafka Streams INNER stream-table join:
 *
 * Time | Stream              Table         | Join output
 * -----+-----------------------------------+-------------------------
 * 10   | ("alice", 999.99)                 | -
 * 20   |                     ("alice", 1L) | -
 * 30   | ("alice", 555.55)                 | ("alice", (555.55, 1L))
 *
 * Kafka Streams LEFT stream-table join:
 *
 * Time | Stream              Table         | Join output
 * -----+-----------------------------------+-------------------------
 * 10   | ("alice", 999.99)                 | ("alice", (999.99, null)
 * 20   |                     ("alice", 1L) | -
 * 30   | ("alice", 555.55)                 | ("alice", (555.55, 1L))
 *
 *
 * The code in this example changes the above behavior so that an application will wait a configurable amount of time
 * for data to arrive also on the table before it produces the join output for a given key (here: "alice").  Depending
 * on your use case, you might prefer this changed behavior over the default join semantics of Kafka Streams.
 *
 * Time | Stream              Table         | Join output
 * -----+-----------------------------------+-------------------------
 * 10   | ("alice", 999.99)                 | -
 * 20   |                     ("alice", 1L) | ("alice", (999.99, 1L))
 * 30   | ("alice", 555.55)                 | ("alice", (555.55, 1L))
 *
 * Note: This example works with Java 8+ only.
 */
public class CustomJoinWithTableTriggeringStreamTableJoinIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopicForStream = "inputTopicForStream";
  private static final String inputTopicForTable = "inputTopicForTable";
  private static final String outputTopic = "outputTopic";
  private static final String tableStoreName = "table-store";

  @BeforeClass
  public static void startKafkaCluster() {
    CLUSTER.createTopic(inputTopicForStream);
    CLUSTER.createTopic(inputTopicForTable);
    CLUSTER.createTopic(outputTopic);
  }

  private static final class StreamTableJoinStreamSideTransformerSupplier
    implements TransformerSupplier<String, Double, KeyValue<String, Pair<Double, Long>>> {

    private final Duration maxWaitTimePerRecordForTableSideData;
    private final Duration frequencyToCheckForExpiredWaitTimes;
    private final String streamBufferStoreName;
    private final String tableStoreName;

    StreamTableJoinStreamSideTransformerSupplier(final Duration maxWaitTimePerRecordForTableSideData,
                                                 final Duration frequencyToCheckForExpiredWaitTimes,
                                                 final String streamBufferStoreName,
                                                 final String tableStoreName) {
      this.maxWaitTimePerRecordForTableSideData = maxWaitTimePerRecordForTableSideData;
      this.frequencyToCheckForExpiredWaitTimes = frequencyToCheckForExpiredWaitTimes;
      this.streamBufferStoreName = streamBufferStoreName;
      this.tableStoreName = tableStoreName;
    }

    @Override
    public Transformer<String, Double, KeyValue<String, Pair<Double, Long>>> get() {
      return new Transformer<String, Double, KeyValue<String, Pair<Double, Long>>>() {

        private KeyValueStore<String, Pair<Double, Instant>> streamBufferStore;
        private KeyValueStore<String, Long> tableStore;
        private ProcessorContext context;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
          streamBufferStore = (KeyValueStore<String, Pair<Double, Instant>>) context.getStateStore(streamBufferStoreName);
          tableStore = (KeyValueStore<String, Long>) context.getStateStore(tableStoreName);
          this.context = context;
          // Note: In practice, you will probably want to use `PunctuationType.STREAM_TIME`.  However, using stream time
          // would make the test/validation setup in this example more complicated, hence we use wall clock time.
          this.context.schedule(frequencyToCheckForExpiredWaitTimes, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
        }

        @Override
        public KeyValue<String, Pair<Double, Long>> transform(final String key, final Double value) {
          return sendFullJoinRecordOrWaitForTableSide(key, value);
        }

        private KeyValue<String, Pair<Double, Long>> sendFullJoinRecordOrWaitForTableSide(final String key, final Double value) {
          final Long tableValue = tableStore.get(key);
          if (tableValue != null) {
            // We have data for both the stream and the table, so we can send a fully populated join message downstream
            // immediately.
            return KeyValue.pair(key, new Pair<>(value, tableValue));
          } else {
            // Don't send a join output just yet because we're still lacking table-side information.  Instead, buffer
            // the current stream-side record, hoping that the table side will eventually see a matching record within
            // `maxWaitTimePerRecordForTableSideData`.
            streamBufferStore.put(key, new Pair<>(value, Instant.now()));
            return null;
          }
        }

        private void punctuate(final long timestamp) {
          sendAndPurgeAnyWaitingRecordsThatHaveExceededWaitTime(timestamp);
        }

        private void sendAndPurgeAnyWaitingRecordsThatHaveExceededWaitTime(final long timestamp) {
          try (KeyValueIterator<String, Pair<Double, Instant>> iterator = streamBufferStore.all()) {
            while (iterator.hasNext()) {
              final KeyValue<String, Pair<Double, Instant>> record = iterator.next();
              final Duration delta = Duration.between(record.value.y, Instant.ofEpochMilli(timestamp));
              if (delta.compareTo(maxWaitTimePerRecordForTableSideData) > 0) {
                // Final attempt to fetch table-side data; we use that data even if it is null (indicates: missing).
                final Long tableValue = tableStore.get(record.key);
                context.forward(record.key, new Pair<>(record.value.x, tableValue));
                streamBufferStore.delete(record.key);
              }
            }
          }
        }

        @Override
        public void close() {
        }

      };
    }

  }

  private static final class StreamTableJoinTableSideValueTransformerWithKeySupplier
    implements ValueTransformerWithKeySupplier<String, Long, Pair<Double, Long>> {

    final private String streamBufferStoreName;

    StreamTableJoinTableSideValueTransformerWithKeySupplier(final String streamBufferStoreName) {
      this.streamBufferStoreName = streamBufferStoreName;
    }

    @Override
    public ValueTransformerWithKey<String, Long, Pair<Double, Long>> get() {
      return new ValueTransformerWithKey<String, Long, Pair<Double, Long>>() {

        private KeyValueStore<String, Pair<Double, Instant>> streamBufferStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
          streamBufferStore = (KeyValueStore<String, Pair<Double, Instant>>) context.getStateStore(streamBufferStoreName);
        }

        @Override
        public Pair<Double, Long> transform(final String key, final Long value) {
          return sendJoinRecordOrWaitForStreamSide(key, value);
        }

        private Pair<Double, Long> sendJoinRecordOrWaitForStreamSide(final String key, final Long value) {
          if (value != null) {
            final Pair<Double, Instant> streamValue = streamBufferStore.get(key);
            if (streamValue != null) {
              // We have data from both stream and table, so we can send a fully populated join message downstream.
              streamBufferStore.delete(key);
              return new Pair<>(streamValue.x, value);
            } else {
              return null;
            }
          } else {
            return null;
          }
        }

        @Override
        public void close() {
        }

      };
    }

  }

  @Test
  public void shouldTriggerStreamTableJoinFromTable() throws Exception {

    final List<KeyValue<String, Double>> inputStreamRecords = Arrays.asList(
      new KeyValue<>("bob", 888.88),
      new KeyValue<>("alice", 999.99)
    );

    final List<KeyValue<String, Long>> inputTableRecords = Arrays.asList(
      new KeyValue<>("alice", 1L),
      new KeyValue<>("alice", 2L)
    );

    final List<KeyValue<String, Pair<Double, Long>>> expectedRecords = Arrays.asList(
      // The join output will have 2L (not 1L) for the table side.  That's because, by default, the DSL enables record
      // caching for tables, which will cause the ValueTransformerWithKey for the KTable to not see the 1L value.
      //
      // If your use case needs to see a join output with value 1L, there are several options available.
      //
      // 1) Set `StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG` to `0`.  This will disable record caching,
      //    and make the table's ValueTransformerWithKey to see the 1L value.  The downside of this approach is that,
      //    by definition, record caching is disabled for the complete topology, resulting in a larger volume of
      //    "intermediate" updates.
      // 2) Don't use a KTable and a ValueTransformerWithKeySupplier for managing the table-side triggering for the
      //    join.  Instead, read the table's topic into a KStream, and then use a normal Transformer with code very
      //    similar to what's implement in StreamTableJoinStreamSideTransformerSupplier.  You must also create a second
      //    state store, managed by this new table-side Transformer, to manage the table-side store manually (because
      //    you use a KStream instead of a KTable for the table's data).  Now you have full control over when and how to
      //    perform table-side join triggering.
      //
      // Note: Kafka Streams' current stream-table join semantics dictate that only a single join output will ever be
      //       produced for a newly arriving stream-side record.  Table-side triggering of the join should only be used
      //       to ensure (rather: to increase the chance) that, when a join output is actually produced, it contains
      //       data from both the stream and the table.  However, table-side triggering should NOT be used to sent
      //       multiple join outputs for the same stream-side record.
      //
      //       Example of wrong table-side join triggering:
      //
      //          Time | Stream              Table         | Join output
      //          -----+-----------------------------------+------------------------
      //          10   | ("alice", 999.99)                 |
      //          20   |                     ("alice", 1L) | ("alice", (999.99, 1L))
      //          30   |                     ("alice", 2L) | ("alice", (999.99, 2L))
      //
      //       In the wrong example above, only one join output must be produced, not two.  It's up to you to decide
      //       which one, however.
      //
      new KeyValue<>("alice", new Pair<>(999.99, 2L)),
      new KeyValue<>("bob", new Pair<>(888.88, null))
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-trigger-join-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    // This state store is used to temporarily buffer any records arriving at the stream side of the join, so that
    // we can wait (if needed) on matching data to arrive at the table side.
    final StoreBuilder<KeyValueStore<String, Pair<Double, Instant>>> streamBufferStateStore =
      Stores
        .keyValueStoreBuilder(
          Stores.persistentKeyValueStore("stream-buffer-state-store"),
          Serdes.String(),
          new PairSerde<>(Serdes.Double(), new InstantSerde())
        )
        .withCachingEnabled();
    builder.addStateStore(streamBufferStateStore);

    // Read the input data.
    final KStream<String, Double> stream = builder.stream(inputTopicForStream, Consumed.with(Serdes.String(), Serdes.Double()));
    final KTable<String, Long> table =
      builder.table(inputTopicForTable, Consumed.with(Serdes.String(), Serdes.Long()), Materialized.as(tableStoreName));

    // Perform the custom join operation.
    final Duration maxWaitTimePerRecordForTableSideData = Duration.ofSeconds(5);
    final Duration frequencyToCheckForExpiredWaitTimes = Duration.ofSeconds(2);
    final KStream<String, Pair<Double, Long>> transformedStream =
      stream.transform(
        new StreamTableJoinStreamSideTransformerSupplier(
          maxWaitTimePerRecordForTableSideData,
          frequencyToCheckForExpiredWaitTimes,
          streamBufferStateStore.name(), tableStoreName),
        streamBufferStateStore.name(), tableStoreName);
    final KTable<String, Pair<Double, Long>> transformedTable =
      table.transformValues(
        new StreamTableJoinTableSideValueTransformerWithKeySupplier(streamBufferStateStore.name()),
        streamBufferStateStore.name());
    final KStream<String, Pair<Double, Long>> mergedStream = transformedStream.merge(transformedTable.toStream());

    // Write the join results back to Kafka.
    mergedStream.to(outputTopic, Produced.with(Serdes.String(), new PairSerde<>(Serdes.Double(), Serdes.Long())));

    // Start the topology.
    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    //
    // Step 2: Produce some input data to the input topics.
    //

    // Produce input data for the stream
    final Properties producerConfigStream = new Properties();
    producerConfigStream.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfigStream.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigStream.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfigStream.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigStream.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
    IntegrationTestUtils.produceKeyValuesSynchronously(inputTopicForStream, inputStreamRecords, producerConfigStream);

    // Produce input data for the table
    final Properties producerConfigTable = new Properties();
    producerConfigTable.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfigTable.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigTable.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfigTable.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigTable.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    IntegrationTestUtils.produceKeyValuesSynchronously(inputTopicForTable, inputTableRecords, producerConfigTable);

    //
    // Step 3: Verify the application's output data.
    //
    final Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "table-trigger-join-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PairOfDoubleAndLongDeserializer.class);
    final List<KeyValue<String, Long>> actualRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
      consumerConfig,
      outputTopic,
      expectedRecords.size()
    );
    streams.close();
    assertThat(actualRecords).isEqualTo(expectedRecords);
  }

}