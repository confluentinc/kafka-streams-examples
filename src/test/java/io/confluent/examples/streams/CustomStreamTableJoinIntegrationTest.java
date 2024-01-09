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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.utils.KeyValueWithTimestamp;
import io.confluent.examples.streams.utils.Pair;
import io.confluent.examples.streams.utils.PairOfDoubleAndLongDeserializer;
import io.confluent.examples.streams.utils.PairSerde;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates one way to implement a custom join operation.
 * Here, we implement custom Transformers with the Processor API and plug them into a DSL topology.
 *
 * Note: This example works with Java 8+ only.
 *
 * TODO (and note for maintainers): This test can be migrated to use the TopologyTestDriver of Kafka Streams only once
 * the driver is handling timestamps exactly like a "real" setup does. Until then, this test must use an embedded
 * Kafka cluster.
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
    final Duration joinWindow = Duration.ofMillis(1000);

    final List<KeyValueWithTimestamp<String, Double>> inputStreamRecords = Arrays.asList(
        new KeyValueWithTimestamp<>("alice", 999.99, TimeUnit.MILLISECONDS.toMillis(10)),
        new KeyValueWithTimestamp<>("bobby", 222.22, TimeUnit.MILLISECONDS.toMillis(15)),
        new KeyValueWithTimestamp<>("alice", 555.55, TimeUnit.MILLISECONDS.toMillis(30)),
        new KeyValueWithTimestamp<>("alice", 666.66, TimeUnit.MILLISECONDS.toMillis(40)),
        new KeyValueWithTimestamp<>("bobby", 111.11, TimeUnit.MILLISECONDS.toMillis(1060))
    );

    final List<KeyValueWithTimestamp<String, Long>> inputTableRecords = Arrays.asList(
        new KeyValueWithTimestamp<>("alice", 1L, TimeUnit.MILLISECONDS.toMillis(20)),
        new KeyValueWithTimestamp<>("alice", 2L, TimeUnit.MILLISECONDS.toMillis(39)),
        new KeyValueWithTimestamp<>("bobby", 8L, TimeUnit.MILLISECONDS.toMillis(50))
    );

    final List<KeyValue<String, Pair<Double, Long>>> expectedOutputRecords = Arrays.asList(
        new KeyValue<>("alice", new Pair<>(999.99, null)),
        new KeyValue<>("bobby", new Pair<>(222.22, null)),
        new KeyValue<>("alice", new Pair<>(555.55, 1L)),
        new KeyValue<>("alice", new Pair<>(666.66, 2L)),
        new KeyValue<>("bobby", new Pair<>(111.11, null))
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
    streamsConfiguration.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 5000);

    // Read the input data.
    final KStream<String, Double> stream = builder.stream(inputTopicForStream, Consumed.with(Serdes.String(), Serdes.Double()));
    builder.table(inputTopicForTable, Consumed.with(Serdes.String(), Serdes.Long()), Materialized.as(tableStoreName));

    // Perform the custom join operation.
    final KStream<String, Pair<Double, Long>> joined =
        stream.transform(
            new StreamTableJoinStreamSideLogic(joinWindow, tableStoreName),
            tableStoreName);

    // Write the join results back to Kafka.
    joined.to(outputTopic, Produced.with(Serdes.String(), new PairSerde<>(Serdes.Double(), Serdes.Long())));

    try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)) {
      final CountDownLatch startLatch = new CountDownLatch(1);
      streams.setStateListener((newState, oldState) -> {
        if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
          startLatch.countDown();
        }

      });

      // Start the topology.
      streams.start();

      try {
        if (!startLatch.await(60, TimeUnit.SECONDS)) {
          throw new RuntimeException("Streams never finished rebalancing on startup");
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      //
      // Step 2: Produce some input data to the input topics.
      //
      writeInputDataToStream(inputStreamRecords);
      writeInputDataToTable(inputTableRecords);

      //
      // Step 3: Verify the application's output data.
      //
      final List<KeyValue<String, Pair<Double, Long>>> actualRecords =
          readOutputDataFromJoinedStream(expectedOutputRecords.size());
      assertThat(actualRecords, equalTo(expectedOutputRecords));
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

    private final Duration joinWindow;
    private final String tableStoreName;

    StreamTableJoinStreamSideLogic(final Duration joinWindow, final String tableStoreName) {
      this.joinWindow = joinWindow;
      this.tableStoreName = tableStoreName;
    }

    @Override
    public Transformer<String, Double, KeyValue<String, Pair<Double, Long>>> get() {
      return new Transformer<String, Double, KeyValue<String, Pair<Double, Long>>>() {

        private KeyValueStore<String, ValueAndTimestamp<Long>> tableStore;
        private ProcessorContext context;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
          tableStore = (KeyValueStore<String, ValueAndTimestamp<Long>>) context.getStateStore(tableStoreName);
          this.context = context;
        }

        @Override
        public KeyValue<String, Pair<Double, Long>> transform(final String key, final Double value) {
          LOG.info("Received stream record ({}, {}) with timestamp {}", key, value, context.timestamp());
          return sendFullJoinRecordOrWaitForTableSide(key, value, context.timestamp());
        }

        private KeyValue<String, Pair<Double, Long>> sendFullJoinRecordOrWaitForTableSide(final String key,
                                                                                          final Double value,
                                                                                          final long streamRecordTimestamp) {
          final ValueAndTimestamp<Long> tableValue = tableStore.get(key);
          if (tableValue != null &&
              withinAcceptableBounds(Instant.ofEpochMilli(tableValue.timestamp()), Instant.ofEpochMilli(streamRecordTimestamp))) {
            final KeyValue<String, Pair<Double, Long>> joinRecord = KeyValue.pair(key, new Pair<>(value, tableValue.value()));
            LOG.info("Table data available for key {}, sending fully populated join message {}", key, joinRecord);
            return joinRecord;
          } else {
            LOG.info("Table data unavailable for key {}, sending the join result as null", key);
            return KeyValue.pair(key, new Pair<>(value, null));
          }
        }

        private boolean withinAcceptableBounds(final Instant streamRecordTimestamp,
                                               final Instant tableRecordTimestamp) {
          return Duration.between(streamRecordTimestamp, tableRecordTimestamp)
              .compareTo(joinWindow) <= 0;
        }

        @Override
        public void close() {}
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

  private List<KeyValue<String, Pair<Double, Long>>> readOutputDataFromJoinedStream(final int numExpectedRecords)
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