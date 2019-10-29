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

import org.apache.kafka.common.serialization.LongDeserializer;
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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that shows how to use state stores in the Kafka Streams DSL.
 * <p>
 * Don't pay too much attention to the output data of the application (or to the output data of the
 * Transformer).  What we want to showcase here is the technical interaction between state stores
 * and the Kafka Streams DSL, at the example of {@link KStream#transform(TransformerSupplier,
 * String...)}.  What the application is actually computing is of secondary concern.
 * <p>
 * Note: This example works with Java 8+ only.
 */
public class StateStoresInTheDSLIntegrationTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  /**
   * Returns a transformer that computes running, ever-incrementing word counts.
   */
  private static final class WordCountTransformerSupplier
    implements TransformerSupplier<byte[], String, KeyValue<String, Long>> {

    final private String stateStoreName;

    WordCountTransformerSupplier(final String stateStoreName) {
      this.stateStoreName = stateStoreName;
    }

    @Override
    public Transformer<byte[], String, KeyValue<String, Long>> get() {
      return new Transformer<byte[], String, KeyValue<String, Long>>() {

        private KeyValueStore<String, Long> stateStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
          stateStore = (KeyValueStore<String, Long>) context.getStateStore(stateStoreName);
        }

        @Override
        public KeyValue<String, Long> transform(final byte[] key, final String value) {
          // For simplification (and unlike the traditional wordcount) we assume that the value is
          // a single word, i.e. we don't split the value by whitespace into potentially one or more
          // words.
          final Optional<Long> count = Optional.ofNullable(stateStore.get(value));
          final Long incrementedCount = count.orElse(0L) + 1;
          stateStore.put(value, incrementedCount);
          return KeyValue.pair(value, incrementedCount);
        }

        @Override
        public void close() {
          // Note: The store should NOT be closed manually here via `stateStore.close()`!
          // The Kafka Streams API will automatically close stores when necessary.
        }
      };
    }

  }

  @Test
  public void shouldAllowStateStoreAccessFromDSL() {
    final List<String> inputValues = Arrays.asList(
      "foo",
      "bar",
      "foo",
      "quux",
      "bar",
      "foo");

    final List<KeyValue<String, Long>> expectedRecords = Arrays.asList(
      new KeyValue<>("foo", 1L),
      new KeyValue<>("bar", 1L),
      new KeyValue<>("foo", 2L),
      new KeyValue<>("quux", 1L),
      new KeyValue<>("bar", 2L),
      new KeyValue<>("foo", 3L)
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-dsl-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    // Create a state store manually.
    final StoreBuilder<KeyValueStore<String, Long>> wordCountsStore = Stores
      .keyValueStoreBuilder(
        Stores.persistentKeyValueStore("WordCountsStore"),
        Serdes.String(),
        Serdes.Long())
      .withCachingEnabled();

    // Important (1 of 2): You must add the state store to the topology, otherwise your application
    // will fail at run-time (because the state store is referred to in `transform()` below.
    builder.addStateStore(wordCountsStore);

    // Read the input data.  (In this example we ignore whatever is stored in the record keys.)
    final KStream<byte[], String> words = builder.stream(inputTopic);

    // Important (2 of 2):  When we call `transform()` we must provide the name of the state store
    // that is going to be used by the `Transformer` returned by `WordCountTransformerSupplier` as
    // the second parameter of `transform()` (note: we are also passing the state store name to the
    // constructor of `WordCountTransformerSupplier`, which we do primarily for cleaner code).
    // Otherwise our application will fail at run-time when attempting to operate on the state store
    // (within the transformer) because `ProcessorContext#getStateStore("WordCountsStore")` will
    // return `null`.
    final KStream<String, Long> wordCounts =
      words.transform(new WordCountTransformerSupplier(wordCountsStore.name()), wordCountsStore.name());

    wordCounts.to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<Void, String> input = topologyTestDriver
        .createInputTopic(inputTopic,
                          new IntegrationTestUtils.NothingSerde<>(),
                          new StringSerializer());
      final TestOutputTopic<String, Long> output = topologyTestDriver
        .createOutputTopic(outputTopic, new StringDeserializer(), new LongDeserializer());
      //
      // Step 3: Produce some input data to the input topic.
      //
      input.pipeValueList(inputValues);

      //
      // Step 4: Verify the application's output data.
      //
      assertThat(output.readKeyValuesToList()).isEqualTo(expectedRecords);
    }
  }

}
