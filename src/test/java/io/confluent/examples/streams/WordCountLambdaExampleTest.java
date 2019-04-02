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

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.*;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stream processing unit test of {@link WordCountLambdaExample}, using TopologyTestDriver.
 *
 * See {@link WordCountLambdaExample} for further documentation.
 *
 * See {@link WordCountLambdaIntegrationTest} for the End-to-end integration test using an embedded Kafkacluster.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class WordCountLambdaExampleTest {

  private TopologyTestDriver testDriver;

  private StringDeserializer stringDeserializer = new StringDeserializer();
  private LongDeserializer longDeserializer = new LongDeserializer();
  private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

  @Before
  public void setup() throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    final StreamsBuilder builder = new StreamsBuilder();
    //Create Actual Stream Processing pipeline
    WordCountLambdaExample.createWordCountStream(builder);
    testDriver = new TopologyTestDriver(builder.build(), WordCountLambdaExample.getStreamsConfiguration("localhost:9092"));
  }

  @After
  public void tearDown() {
    try {
      testDriver.close();
    } catch (RuntimeException e) {
      // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
      // Logged stacktrace cannot be avoided
      System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
    }
  }


  /** Read one Record from output topic.
   *
   * @return ProducerRecord containing word as key and count as value
   */
  private ProducerRecord<String, Long> readOutput() {
    return testDriver.readOutput(WordCountLambdaExample.outputTopic, stringDeserializer, longDeserializer);
  }

  /** Read counts from output to map.
   * If existing word is incremented, it can appear twice in output and is replaced in map
   *
   * @return Map of Word and counts
   */
  private Map<String, Long> getOutputList() {
    Map<String, Long> output = new HashMap<>();
    ProducerRecord<String, Long> outputRow;
    while((outputRow = readOutput()) != null) {
      output.put(outputRow.key(), outputRow.value());
    }
    return output;
  }

  /** Simple test validating count of one word
   */
  @Test
  public void testOneWord() {
    String nullKey = null;
    //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
    testDriver.pipeInput(recordFactory.create(WordCountLambdaExample.inputTopic, nullKey, "Hello", 1L));
    //Read and validate output
    ProducerRecord<String, Long> output = readOutput();
    OutputVerifier.compareKeyValue(output, "hello", 1L);
    //No more output in topic
    assertThat(readOutput()).isNull();
  }

  @Test
  public void shouldCountWords() throws Exception {
    final List<String> inputValues = Arrays.asList(
        "Hello Kafka Streams",
        "All streams lead to Kafka",
        "Join Kafka Summit",
        "И теперь пошли русские слова"
    );
    final Map<String, Long> expectedWordCounts = new HashMap<>();
    expectedWordCounts.put("hello", 1L);
    expectedWordCounts.put("all", 1L);
    expectedWordCounts.put("streams", 2L);
    expectedWordCounts.put("lead", 1L);
    expectedWordCounts.put("to", 1L);
    expectedWordCounts.put("join", 1L);
    expectedWordCounts.put("kafka", 3L);
    expectedWordCounts.put("summit", 1L);
    expectedWordCounts.put("и", 1L);
    expectedWordCounts.put("теперь", 1L);
    expectedWordCounts.put("пошли", 1L);
    expectedWordCounts.put("русские", 1L);
    expectedWordCounts.put("слова", 1L);

    List<KeyValue<String, String>> records = inputValues.stream().map(v -> new KeyValue<String, String>(null, v)).collect(Collectors.toList());
    testDriver.pipeInput(recordFactory.create(WordCountLambdaExample.inputTopic, records, 1L, 100L));

    final Map<String, Long> actualWordCounts = getOutputList();
    assertThat(actualWordCounts).containsAllEntriesOf(expectedWordCounts);
  }

}
