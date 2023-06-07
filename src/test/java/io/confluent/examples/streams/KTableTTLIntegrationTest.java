/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.examples.streams;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
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
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.utils.KeyValueWithTimestamp;
import io.confluent.examples.streams.utils.Pair;
import io.confluent.examples.streams.utils.PairOfDoubleAndLongDeserializer;
import io.confluent.examples.streams.utils.PairSerde;

/**
 * End-to-end integration test that demonstrates how to delete data from KTable (via the upstream
 * topic) using TTLs. This is based on Mathias J Sax's example from 
 * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Stream+Usage+Patterns
 * 
 * Note: This example works with Java 8+ only.
 *
 * 
 */
public class KTableTTLIntegrationTest {

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
        new KeyValueWithTimestamp<>("alice", 666.66, TimeUnit.MILLISECONDS.toMillis(70006)),
        new KeyValueWithTimestamp<>("bobby", 111.11, TimeUnit.MILLISECONDS.toMillis(70016)));

    final List<KeyValueWithTimestamp<String, Long>> inputTableRecords =
        Arrays.asList(new KeyValueWithTimestamp<>("alice", 1L, TimeUnit.MILLISECONDS.toMillis(5)),
            new KeyValueWithTimestamp<>("bobby", 2L, TimeUnit.MILLISECONDS.toMillis(10)),
            new KeyValueWithTimestamp<>("freddy", 3L, TimeUnit.MILLISECONDS.toMillis(65000)) // just moving the clock along
            );

    final List<KeyValue<String, Pair<Double, Long>>> expectedOutputRecords =
        Arrays.asList(new KeyValue<>("alice", new Pair<>(999.99, 1L)),
            new KeyValue<>("bobby", new Pair<>(222.22, 2L)),
            new KeyValue<>("alice", new Pair<>(555.55, 1L)),
            new KeyValue<>("alice", new Pair<>(666.66, null)),
            new KeyValue<>("bobby", new Pair<>(111.11, null)));

    final List<KeyValue<String, Long>> expectedInputTopicRecords =
        Arrays.asList(new KeyValue<>("alice", 1L),
            new KeyValue<>("bobby", 2L),
            new KeyValue<>("freddy", 3L),
            new KeyValue<>("alice", null),
            new KeyValue<>("bobby", null));


    try (final KafkaStreams streams = new KafkaStreams(buildTopology1(), getStreamProps1());
         final KafkaStreams streams2 = new KafkaStreams(buildTopology2(), getStreamProps2())) {
      final CountDownLatch startLatch = new CountDownLatch(2);
      streams.setStateListener((newState, oldState) -> {
        if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
          startLatch.countDown();
        }

      });
      
      streams2.setStateListener((newState, oldState) -> {
        if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
          startLatch.countDown();
        }

      });

      // Start the topology.
      streams.start();
      streams2.start(); // here is where all the TTLing will happen. If you want to do it in the same topology
                        // you might have to fiddle with commit interval and cache max buffer size

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
      writeInputDataToTable(inputTableRecords);
      writeInputDataToStream(inputStreamRecords);

      //
      // Step 3: Verify that the joining is happening correctly with respect to the KTable records being
      // TTLed
      //
      final List<KeyValue<String, Pair<Double, Long>>> actualRecords =
          readOutputDataFromJoinedStream(expectedOutputRecords.size());
      assertThat(actualRecords, equalTo(expectedOutputRecords));
      
      // Check that the null values are being written into the input topic for table
      final List<KeyValue<String,Long>> inputTableRecordsFromTopic = readInputTablTopic(5);
      assertThat(inputTableRecordsFromTopic, equalTo(expectedInputTopicRecords));
      
    }
  }

  private Topology buildTopology2() {
    final StreamsBuilder builder2 = new StreamsBuilder();
    // Setting tombstones for records seen past a TTL of MAX_AGE
    final Duration MAX_AGE = Duration.ofMinutes(1);
    final Duration SCAN_FREQUENCY = Duration.ofSeconds(5);
    final String STATE_STORE_NAME = "ktable-ttl-integration-test-purge-worker-store";
    
 
    // adding a custom state store for the TTL transformer which has a key of type string, and a value of type long
    // which represents the timestamp
    final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STATE_STORE_NAME),
        Serdes.String()/* keys are long valued in this example */,
        Serdes.Long()/*
                      * we store only the record timestamp, since it's all we're basing the purge on
                      */
    );
   

    builder2.addStateStore(storeBuilder);

    // tap the table topic in order to insert a tombstone after MAX_AGE based on event time
    builder2.stream(inputTopicForTable,  Consumed.with(Serdes.String(), Serdes.Long()))
           .transform(new TTLTransformerSupplier<String,Long,KeyValue<String,Long>>(MAX_AGE, SCAN_FREQUENCY, STATE_STORE_NAME), STATE_STORE_NAME)
           .to(inputTopicForTable, Produced.with(Serdes.String(), Serdes.Long())); //write the tombstones back out to the input topic
    return builder2.build();
  }

  private Topology buildTopology1() {
    final StreamsBuilder builder = new StreamsBuilder();

  
    // Read the input data.
    final KStream<String, Double> stream =
        builder.stream(inputTopicForStream, Consumed.with(Serdes.String(), Serdes.Double()));
    final KTable<String, Long> table = builder.table(inputTopicForTable,
        Consumed.with(Serdes.String(), Serdes.Long()), Materialized.as(tableStoreName));


    
    // Perform the custom join operation.
    final KStream<String, Pair<Double, Long>> joined = stream.leftJoin(table, (left, right) -> {
      if (right != null)
        return new Pair<>(left, right);
      return new Pair<>(left, null);
    });
    // Write the join results back to Kafka.
    joined.to(outputTopic,
        Produced.with(Serdes.String(), new PairSerde<>(Serdes.Double(), Serdes.Long())));
    return builder.build();
  }
  

  private Properties getStreamProps1() {
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ttl-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the
    // test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,
        TestUtils.tempDirectory().getAbsolutePath());
    streamsConfiguration.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 5000);
    return streamsConfiguration;
  }

  private Properties getStreamProps2() {
    final Properties streamsConfiguration2 = new Properties();
    streamsConfiguration2.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ttl-integration-test-2");
    streamsConfiguration2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the
    // test.
    streamsConfiguration2.put(StreamsConfig.STATE_DIR_CONFIG,
        TestUtils.tempDirectory().getAbsolutePath()+"_2");
    return streamsConfiguration2;
  }



  private static final class TTLTransformerSupplier<K,V,E>
      implements TransformerSupplier<K, V, KeyValue<K, V>> {

    private final Duration maxAge;
    private final Duration scanFrequency;
    private final String purgeStoreName;

    public TTLTransformerSupplier(final Duration maxAge, final Duration scanFrequency, final String stateStoreName) {
      this.maxAge = maxAge;
      this.scanFrequency = scanFrequency;
      this.purgeStoreName = stateStoreName;
    }

    @Override
    public Transformer<K, V, KeyValue<K, V>> get() {

      return new Transformer<K, V, KeyValue<K, V>>() {
        private ProcessorContext context;
        private KeyValueStore<K, Long> stateStore;

        @Override
        public void init(final ProcessorContext context) {
          this.context = context;
          this.stateStore = (KeyValueStore<K, Long>) context.getStateStore(purgeStoreName);
          // This is where the magic happens. This causes Streams to invoke the Punctuator
          // on an interval, using stream time. That is, time is only advanced by the record
          // timestamps
          // that Streams observes. This has several advantages over wall-clock time for this
          // application:
          // * It'll produce the exact same sequence of updates given the same sequence of data.
          // This seems nice, since the purpose is to modify the data stream itself, you want to
          // have
          // a clear understanding of when stuff is going to get deleted. For example, if something
          // breaks down upstream for this topic, and it stops getting new data for a while, wall
          // clock
          // time would just keep deleting data on schedule, whereas stream time will wait for
          // new updates to come in.
          // You can change to wall clock time here if that is what is needed
          context.schedule(scanFrequency, PunctuationType.STREAM_TIME, timestamp -> {
            final long cutoff = timestamp - maxAge.toMillis();

            // scan over all the keys in this partition's store
            // this can be optimized, but just keeping it simple.
            // this might take a while, so the Streams timeouts should take this into account
            try (final KeyValueIterator<K, Long> all = stateStore.all()) {
              while (all.hasNext()) {
                final KeyValue<K, Long> record = all.next();
                if (record.value != null && record.value < cutoff) {
                  // if a record's last update was older than our cutoff, emit a tombstone.
                  context.forward(record.key, null);
                }
              }
            }
          });
        }

        @Override
        public KeyValue<K, V> transform(final K key, final V value) {
          // this gets invoked for each new record we consume. If it's a tombstone, delete
          // it from our state store. Otherwise, store the record timestamp.
          if (value == null) {
            stateStore.delete(key);
          } else {
            stateStore.put(key, context.timestamp());
          }
          return null; // no need to return anything here. the punctuator will emit the tombstones
                       // when necessary
        }

        @Override
        public void close() {} // no need to close anything; Streams already closes the state store.

      };
    };


  }

  private void writeInputDataToStream(
      final List<KeyValueWithTimestamp<String, Double>> inputStreamRecords)
      throws ExecutionException, InterruptedException {
    // Produce input data for the stream
    final Properties producerConfigStream = new Properties();
    producerConfigStream.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfigStream.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigStream.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfigStream.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigStream.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
    IntegrationTestUtils.produceKeyValuesWithTimestampsSynchronously(inputTopicForStream,
        inputStreamRecords, producerConfigStream);
  }

  private void writeInputDataToTable(
      final List<KeyValueWithTimestamp<String, Long>> inputTableRecords)
      throws ExecutionException, InterruptedException {
    final Properties producerConfigTable = new Properties();
    producerConfigTable.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfigTable.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigTable.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfigTable.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigTable.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    IntegrationTestUtils.produceKeyValuesWithTimestampsSynchronously(inputTopicForTable,
        inputTableRecords, producerConfigTable);
  }

  private List<KeyValue<String, Pair<Double, Long>>> readOutputDataFromJoinedStream(
      final int numExpectedRecords) throws InterruptedException {
    final Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG,
        "custom-join-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        PairOfDoubleAndLongDeserializer.class);
    return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic,
        numExpectedRecords);
  }
  
  private List<KeyValue<String, Long>> readInputTablTopic(final int numExpectedRecords)
      throws ExecutionException, InterruptedException {
    final Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG,
        "custom-join-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        LongDeserializer.class);
    return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, inputTopicForTable,
        numExpectedRecords);
  }
}
