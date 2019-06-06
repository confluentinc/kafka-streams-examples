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
import org.apache.kafka.common.utils.Bytes;
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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.utils.InstantSerde;
import io.confluent.examples.streams.utils.KeyValueWithTimestamp;
import io.confluent.examples.streams.utils.Pair;
import io.confluent.examples.streams.utils.PairOfDoubleAndLongDeserializer;
import io.confluent.examples.streams.utils.PairSerde;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates one way to implement a custom join operation.
 * Here, we implement custom Transformers with the Processor API and plug them into a DSL topology.
 *
 * Note: This example works with Java 8+ only.
 *
 * EXAMPLE DESCRIPTION
 * ===================
 * Specifically, this example implements a stream-table LEFT join where both (1) the stream side triggers a join output
 * being sent downstream (default behavior of KStreams) but also (2) the table side (not supported yet in Kafka Streams
 * out of the box).  The example will also delay join output for a configurable amount of time when a record arrives in
 * the stream but doesn't yet have a matching record in the table.  If data happens to arrive "in time" on the table
 * side, a "full" join output will be produced.  If table data does not arrive in time, then (like the default behavior
 * for a stream-table LEFT join) a join output will be sent where the table-side data is `null`.  See the input/output
 * join examples further down below as illustration of the implemented behavior.
 *
 * The approach in this example shares state stores between a stream-side and a table-side transformer, respectively.
 * This is safe because, if shared, Kafka Streams will place the transformers as well as the state stores into the same
 * stream task, in which all access is exclusive and single-threaded.  The state store on the table side is the normal
 * store of a KTable (whereas a second, dedicated state store for the table would have duplicated data unnecessarily),
 * whereas the state store on the stream side is manually added and attached to the processing topology.
 *
 * An alternative, more flexible approach is outlined further down below, in case you need additional control over the
 * join behavior, e.g. by including stream-side vs. table-side timestamps in your decision-making logic.
 *
 *
 * THIS CODE EXAMPLE COMPARED TO KAFKA STREAMS DEFAULT JOIN BEHAVIOR
 * =================================================================
 * The default stream-table join behavior of Kafka Streams only triggers join output when data arrives at the stream
 * side.  See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics.
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
 * for data to arrive also at the table before it produces the join output for a given key (here: "alice").  The
 * motivation is that, in this example, we'd prefer to receive fully populated join messages rather than join messages
 * where the table-side information is missing (null).  Depending on your use case, you might prefer this changed
 * behavior, and these changed semantics, over the default join semantics of Kafka Streams.
 *
 * Time | Stream              Table         | Join output
 * -----+-----------------------------------+-------------------------
 * 10   | ("alice", 999.99)                 | -
 * 20   |                     ("alice", 1L) | ("alice", (999.99, 1L))
 * 30   | ("alice", 555.55)                 | ("alice", (555.55, 1L))
 *
 * Note how, in the example above, the custom join produced a join output of `("alice", (999.99, 1L))`, even though the
 * table-side record `("alice", 1L)` has NEWER timestamp than the stream record `("alice", 999.99)`.
 *
 *
 * YOUR OWN LOGIC SHOULD STILL ADHERE TO GENERAL JOIN RULES
 * ========================================================
 * Kafka Streams' current stream-table join semantics dictate that only a single join output will ever be produced for a
 * newly arriving stream-side record.  Table-side triggering of the join should only be used to ensure (rather: to
 * increase the chance) that, when a join output is actually produced, it contains data from both the stream and the
 * table.  However, table-side triggering should NOT be used to sent multiple join outputs for the same stream-side
 * record.
 *
 * Example of WRONG logic:
 *
 * Time | Stream              Table         | Join output
 * -----+-----------------------------------+------------------------
 * 10   | ("alice", 999.99)                 |
 * 20   |                     ("alice", 1L) | ("alice", (999.99, 1L))
 * 30   |                     ("alice", 2L) | ("alice", (999.99, 2L))
 *
 * The wrong example above falsely produces two outputs, whereas it should produce only a single one.  It's up to you to
 * decide which one, however.
 *
 *
 * HOW TO ADAPT THIS EXAMPLE TO YOUR OWN USE CASES
 * ===============================================
 * 1. You might want to add further logic that, for instance, changes the join behavior depending on the respective
 * timestamps of received stream records and table records.
 *
 * 2. The KTable's ValueTransformerWithKeySupplier can only react to values it <i>actually observes</i>.  By default,
 * the Kafka Streams DSL enables record caching for tables, which will cause the ValueTransformerWithKey to not observe
 * every single value that enters the table.  If your use case requires observing every single record, you must
 * configure the KTable so that its state store disables record caching: `Materialized.as(...).withCachingDisabled()`.
 *
 * 3. If you need even more control on what join output is being produced (or not being produced), or more control on
 * state management for the join in general, you may want to switch from this approach's use of a KTable (for reading
 * the table's topic) together with a ValueTransformerWithKeySupplier for managing the table-side triggering of the join
 * to a KStream together with a normal Transformer and a second state store.  Here, you'd read the table's topic into a
 * KStream, and then use a normal Transformer with code very similar to what's implemented in
 * StreamTableJoinStreamSideLogic.  You must create a second state store, managed by this new table-side Transformer, to
 * manage the table-side store manually (because you use a KStream instead of a KTable for the table's data).
 */
public class CustomStreamTableJoinIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopicForStream = "inputTopicForStream";
  private static final String inputTopicForTable = "inputTopicForTable";
  private static final String outputTopic = "outputTopic";
  private static final String tableStoreName = "table-store";

  @BeforeClass
  public static void startKafkaCluster() throws InterruptedException {
    CLUSTER.createTopic(inputTopicForStream);
    CLUSTER.createTopic(inputTopicForTable);
    CLUSTER.createTopic(outputTopic);
  }

  @Test
  public void shouldTriggerStreamTableJoinFromTable() throws Exception {
    final Duration approxMaxWaitTimePerRecordForTableData = Duration.ofSeconds(5);
    final Duration frequencyToCheckForExpiredWaitTimes = Duration.ofSeconds(2);

    final List<KeyValueWithTimestamp<String, Double>> inputStreamRecords = Arrays.asList(
        new KeyValueWithTimestamp<>("alice", 999.99, TimeUnit.MILLISECONDS.toMillis(10)),
        new KeyValueWithTimestamp<>("bobby", 222.22, TimeUnit.MILLISECONDS.toMillis(15)),
        new KeyValueWithTimestamp<>("alice", 555.55, TimeUnit.MILLISECONDS.toMillis(30)),
        new KeyValueWithTimestamp<>("alice", 666.66, TimeUnit.MILLISECONDS.toMillis(40))
    );

    final List<KeyValueWithTimestamp<String, Long>> inputTableRecords = Arrays.asList(
        new KeyValueWithTimestamp<>("alice", 1L, TimeUnit.MILLISECONDS.toMillis(20)),
        new KeyValueWithTimestamp<>("alice", 2L, TimeUnit.MILLISECONDS.toMillis(39)),
        new KeyValueWithTimestamp<>("bobby", 8L,
            approxMaxWaitTimePerRecordForTableData.plus(Duration.ofSeconds(1)).toMillis())
    );

    final List<KeyValue<String, Pair<Double, Long>>> expectedOutputRecords = Arrays.asList(
        new KeyValue<>("alice", new Pair<>(999.99, 1L)),
        new KeyValue<>("alice", new Pair<>(555.55, 1L)),
        new KeyValue<>("alice", new Pair<>(666.66, 2L)),
        new KeyValue<>("bobby", new Pair<>(222.22, null))
    );

    //
    // Step 1: Define and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "custom-join-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    // This state store is used to temporarily buffer any records arriving at the stream side of the join, so that
    // we can wait (if needed) for matching data to arrive at the table side.
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
        builder.table(inputTopicForTable, Consumed.with(Serdes.String(), Serdes.Long()),
            // Disabling this state store's cache ensures that table-side data is seen more immediately and that reading the
            // stream-side data doesn't advance the stream-time too quickly (which might result in join output being
            // sent by the stream side before the processing topology had the chance to look at the table side).
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(tableStoreName).withCachingDisabled());

    // Perform the custom join operation.
    final KStream<String, Pair<Double, Long>> transformedStream =
        stream.transform(
            new StreamTableJoinStreamSideLogic(
                approxMaxWaitTimePerRecordForTableData,
                frequencyToCheckForExpiredWaitTimes,
                streamBufferStateStore.name(),
                tableStoreName),
            streamBufferStateStore.name(),
            tableStoreName);
    final KTable<String, Pair<Double, Long>> transformedTable =
        table.transformValues(
            new StreamTableJoinTableSideLogic(approxMaxWaitTimePerRecordForTableData, streamBufferStateStore.name()),
            streamBufferStateStore.name());
    final KStream<String, Pair<Double, Long>> joined =
        // We need to discard the table's tombstone records (records with null values) from the stream-table join output.
        // Such tombstone records are present because we are working with a KTable and a `ValueTransformerWithKey`, and
        // the latter is not able to discard these itself.
        transformedStream.merge(transformedTable.toStream().filter((k, v) -> v != null));

    // Write the join results back to Kafka.
    joined.to(outputTopic, Produced.with(Serdes.String(), new PairSerde<>(Serdes.Double(), Serdes.Long())));

    try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)) {
      // Start the topology.
      streams.start();

      //
      // Step 2: Produce some input data to the input topics.
      //
      writeInputDataToStream(inputStreamRecords);
      writeInputDataToTable(inputTableRecords);

      //
      // Step 3: Verify the application's output data.
      //
      final List<KeyValue<String, Long>> actualRecords = readOutputDataFromJoinedStream(expectedOutputRecords.size());
      assertThat(actualRecords).isEqualTo(expectedOutputRecords);
    }
  }

  /**
   * Implements the stream-side join behavior of waiting a configurable amount of time for table-side data to arrive
   * before sending a join output for a newly received stream-side record.  This is but one example for such custom
   * join semantics -- feel free to modify this example to match your own needs.
   *
   * This behavior will increase the likelihood of "fully populated" join output messages, i.e. with data from both the
   * stream and the table side.  The downside is that the waiting behavior will increase the end-to-end processing
   * latency for a stream-side record in the topology.
   */
  private static final class StreamTableJoinStreamSideLogic
      implements TransformerSupplier<String, Double, KeyValue<String, Pair<Double, Long>>> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamTableJoinStreamSideLogic.class);

    private final Duration approxMaxWaitTimePerRecordForTableData;
    private final Duration frequencyToCheckForExpiredWaitTimes;
    private final String streamBufferStoreName;
    private final String tableStoreName;

    StreamTableJoinStreamSideLogic(final Duration approxMaxWaitTimePerRecordForTableData,
                                   final Duration frequencyToCheckForExpiredWaitTimes,
                                   final String streamBufferStoreName,
                                   final String tableStoreName) {
      this.approxMaxWaitTimePerRecordForTableData = approxMaxWaitTimePerRecordForTableData;
      this.frequencyToCheckForExpiredWaitTimes = frequencyToCheckForExpiredWaitTimes;
      this.streamBufferStoreName = streamBufferStoreName;
      this.tableStoreName = tableStoreName;
    }

    @Override
    public Transformer<String, Double, KeyValue<String, Pair<Double, Long>>> get() {
      return new Transformer<String, Double, KeyValue<String, Pair<Double, Long>>>() {

        private KeyValueStore<String, Pair<Double, Instant>> streamBufferStore;
        private KeyValueStore<String, ValueAndTimestamp<Long>> tableStore;
        private ProcessorContext context;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
          streamBufferStore = (KeyValueStore<String, Pair<Double, Instant>>) context.getStateStore(streamBufferStoreName);
          tableStore = (KeyValueStore<String, ValueAndTimestamp<Long>>) context.getStateStore(tableStoreName);
          this.context = context;
          this.context.schedule(frequencyToCheckForExpiredWaitTimes, PunctuationType.STREAM_TIME, this::punctuate);
        }

        @Override
        public KeyValue<String, Pair<Double, Long>> transform(final String key, final Double value) {
          LOG.info("Received stream record ({}, {}) with timestamp {}", key, value, context.timestamp());
          sendAnyWaitingRecordForKey(key);
          return sendFullJoinRecordOrWaitForTableSide(key, value, context.timestamp());
        }

        /**
         * In this example we opt to force-forward any waiting record for a given key when a new record for that key
         * arrives. Alternatively, we could decide to keep buffering such records until either their wait times expire
         * or a table-side record is received.
         */
        private void sendAnyWaitingRecordForKey(final String key) {
          LOG.info("Checking whether there is any waiting stream record for key {} to forward because new stream " +
              "record was received for same key", key);
          final Pair<Double, Instant> streamValue = streamBufferStore.get(key);
          if (streamValue != null) {
            // No need to check whether a table-side record exists. Because if it did, the table side would have
            // already triggered a join update and removed that stream record from the buffer.
            final Pair<Double, Long> joinedValue = new Pair<>(streamValue.x, null);
            LOG.info("Force-forwarding waiting stream record ({}, {}) because new stream record received for key {}",
                key, joinedValue, key);
            context.forward(key, joinedValue);
            streamBufferStore.delete(key);
          }
        }

        private KeyValue<String, Pair<Double, Long>> sendFullJoinRecordOrWaitForTableSide(final String key,
                                                                                          final Double value,
                                                                                          final long streamRecordTimestamp) {
          final ValueAndTimestamp<Long> tableValue = tableStore.get(key);
          if (tableValue != null) {
            final KeyValue<String, Pair<Double, Long>> joinRecord = KeyValue.pair(key, new Pair<>(value, tableValue.value()));
            LOG.info("Table data available for key {}, sending fully populated join message {}", key, joinRecord);
            return joinRecord;
          } else {
            LOG.info("Table data unavailable for key {}, buffering stream record ({}, {}) temporarily", key, key, value);
            streamBufferStore.put(key, new Pair<>(value, Instant.ofEpochMilli(streamRecordTimestamp)));
            return null;
          }
        }

        private void punctuate(final long timestamp) {
          LOG.info("Punctuating @ timestamp {}", timestamp);
          sendAndPurgeAnyWaitingRecordsThatHaveExceededWaitTime(timestamp);
        }

        private void sendAndPurgeAnyWaitingRecordsThatHaveExceededWaitTime(final long currentStreamTime) {
          try (KeyValueIterator<String, Pair<Double, Instant>> iterator = streamBufferStore.all()) {
            while (iterator.hasNext()) {
              final KeyValue<String, Pair<Double, Instant>> record = iterator.next();
              LOG.info("Checking waiting stream record ({}, {}) with timestamp {}", record.key, record.value.x,
                  record.value.y.toEpochMilli());
              if (waitTimeExpired(record.value.y, currentStreamTime)) {
                final Pair<Double, Long> joinedValue = new Pair<>(record.value.x, null);
                LOG.info("Wait time for stream record ({}, {}) expired, force-forwarding now as join message " +
                    "({}, ({}, {}))", record.key, record.value.x, record.key, joinedValue.x, joinedValue.y);
                context.forward(record.key, joinedValue);
                streamBufferStore.delete(record.key);
              }
            }
          }
        }

        private boolean waitTimeExpired(final Instant recordTimestamp, final long currentStreamTime) {
          return Duration.between(recordTimestamp, Instant.ofEpochMilli(currentStreamTime))
              .compareTo(approxMaxWaitTimePerRecordForTableData) > 0;
        }

        @Override
        public void close() {
        }

      };
    }

  }

  /**
   * Implements table-side triggering of join output.  This is but one example for such custom join semantics -- feel
   * free to modify this example to match your own needs.
   *
   * For every <i>observed</i> record arriving at its upstream table, this transformer will check for a buffered (i.e.,
   * not yet joined) record on the stream side.  If there is a match, then the transformer will produce a fully
   * populated join output message -- which is the desired table-side triggering behavior.  If there is no match, then
   * the transformer will effectively do nothing (it will actually send a tombstone record).
   */
  private static final class StreamTableJoinTableSideLogic
      implements ValueTransformerWithKeySupplier<String, Long, Pair<Double, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamTableJoinTableSideLogic.class);

    private final Duration approxMaxWaitTimePerRecordForTableData;
    private final String streamBufferStoreName;

    StreamTableJoinTableSideLogic(final Duration approxMaxWaitTimePerRecordForTableData,
                                  final String streamBufferStoreName) {
      this.approxMaxWaitTimePerRecordForTableData = approxMaxWaitTimePerRecordForTableData;
      this.streamBufferStoreName = streamBufferStoreName;
    }

    @Override
    public ValueTransformerWithKey<String, Long, Pair<Double, Long>> get() {
      return new ValueTransformerWithKey<String, Long, Pair<Double, Long>>() {

        private KeyValueStore<String, Pair<Double, Instant>> streamBufferStore;
        private ProcessorContext context;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
          streamBufferStore = (KeyValueStore<String, Pair<Double, Instant>>) context.getStateStore(streamBufferStoreName);
          this.context = context;
        }

        /**
         * A return value of `null` for this method represents a table tombstone record.  We need to discard such
         * tombstone records in the downstream processing topology (see code further above in this file) because, in the
         * context of this example of a custom stream-table join, they must not appear in the join output.  Note that we
         * can't discard the tombstones directly in the ValueTransformerWithKey.
         *
         * Alternatively, we could switch from the use of `transformValues()` to `transform()` to avoid this issue.
         * However, the benefit of `transformValues()` is that we avoid potential downstream re-partitioning.
         *
         * TODO: In upcoming Apache Kafka 2.3 and Confluent Platform 5.3, we should consider switching this example to
         *       use `flatTransformValues()` (introduced by KIP-313). The example would then change from
         *       `KTable#transformValues()` to a setup with `KTable#toStream()#flatTransformValues()`. This would also
         *       get rid of tombstones in the output because `KStream#flatTransformValues()` may output zero records
         *       for an input record.
         */
        @Override
        public Pair<Double, Long> transform(final String key, final Long value) {
          LOG.info("Received table record ({}, {}) with timestamp {}", key, value, context.timestamp());
          return possiblySendFullJoinRecord(key, value, context.timestamp());
        }

        private Pair<Double, Long> possiblySendFullJoinRecord(final String key,
                                                              final Long tableValue,
                                                              final long tableRecordTimestamp) {
          if (tableValue != null) {
            final Pair<Double, Instant> streamValue = streamBufferStore.get(key);
            if (streamValue != null) {
              // You can also incorporate timestamps of records into your join logic as shown here.
              if (withinAcceptableBounds(streamValue.y, Instant.ofEpochMilli(tableRecordTimestamp))) {
                LOG.info("Stream data available for key {}, sending fully populated join message ({}, ({}, {}))", key,
                    key, streamValue.x, tableValue);
                streamBufferStore.delete(key);
                return new Pair<>(streamValue.x, tableValue);
              } else {
                LOG.info("Stream data available for key {} but not used because it is too old; sending tombstone", key);
                return null;
              }
            } else {
              LOG.info("Stream data not available for key {}; sending tombstone", key);
              return null;
            }
          } else {
            LOG.info("Table value for key {} is null (tombstone); sending tombstone", key);
            return null;
          }
        }

        private boolean withinAcceptableBounds(final Instant streamRecordTimestamp,
                                               final Instant tableRecordTimestamp) {
          return Duration.between(streamRecordTimestamp, tableRecordTimestamp)
              .compareTo(approxMaxWaitTimePerRecordForTableData) <= 0;
        }

        @Override
        public void close() {
        }

      };
    }

  }

  private void writeInputDataToStream(final List<KeyValueWithTimestamp<String, Double>> inputStreamRecords)
      throws ExecutionException, InterruptedException {
    // Produce input data for the stream
    final Properties producerConfigStream = new Properties();
    producerConfigStream.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfigStream.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigStream.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfigStream.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigStream.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
    IntegrationTestUtils.produceKeyValuesWithTimestampsSynchronously(
        inputTopicForStream, inputStreamRecords, producerConfigStream);
  }

  private void writeInputDataToTable(final List<KeyValueWithTimestamp<String, Long>> inputTableRecords)
      throws ExecutionException, InterruptedException {
    final Properties producerConfigTable = new Properties();
    producerConfigTable.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfigTable.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigTable.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfigTable.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigTable.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    IntegrationTestUtils.produceKeyValuesWithTimestampsSynchronously(
        inputTopicForTable, inputTableRecords, producerConfigTable);
  }

  private List<KeyValue<String, Long>> readOutputDataFromJoinedStream(final int numExpectedRecords)
      throws InterruptedException {
    final Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-join-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PairOfDoubleAndLongDeserializer.class);
    return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, numExpectedRecords);
  }

}