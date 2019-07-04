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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.confluent.examples.streams.IntegrationTestUtils.mkEntry;
import static io.confluent.examples.streams.IntegrationTestUtils.mkMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test based on {@link WordCountLambdaExample}, using an embedded Kafka
 * cluster.
 * <p>
 * See {@link WordCountLambdaExample} for further documentation.
 * <p>
 * See {@link WordCountScalaIntegrationTest} for the equivalent Scala example.
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class WordCountLambdaIntegrationTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @Test
  public void shouldCountWords() {
    final List<String> inputValues = Arrays.asList(
      "Hello Kafka Streams",
      "All streams lead to Kafka",
      "Join Kafka Summit",
      "И теперь пошли русские слова"
    );
    final Map<String, Long> expectedWordCounts = mkMap(
      mkEntry("hello", 1L),
      mkEntry("all", 1L),
      mkEntry("streams", 2L),
      mkEntry("lead", 1L),
      mkEntry("to", 1L),
      mkEntry("join", 1L),
      mkEntry("kafka", 3L),
      mkEntry("summit", 1L),
      mkEntry("и", 1L),
      mkEntry("теперь", 1L),
      mkEntry("пошли", 1L),
      mkEntry("русские", 1L),
      mkEntry("слова", 1L)
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> textLines = builder.stream(inputTopic);

    final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

    final KTable<String, Long> wordCounts = textLines
      .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
      // no need to specify explicit serdes because the resulting key and value types match our default serde settings
      .groupBy((key, word) -> word)
      .count();

    wordCounts.toStream().to(outputTopic, Produced.with(stringSerde, longSerde));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Produce some input data to the input topic.
      //
      IntegrationTestUtils.produceKeyValuesSynchronously(
        inputTopic,
        inputValues.stream().map(v -> new KeyValue<>(null, v)).collect(Collectors.toList()),
        topologyTestDriver,
        new IntegrationTestUtils.NothingSerde<>(),
        new StringSerializer()
      );

      //
      // Step 3: Verify the application's output data.
      //
      final Map<String, Long> actualWordCounts = IntegrationTestUtils.drainTableOutput(
        outputTopic,
        topologyTestDriver,
        new StringDeserializer(),
        new LongDeserializer()
      );
      assertThat(actualWordCounts).isEqualTo(expectedWordCounts);
    }
  }

}
