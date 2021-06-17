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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between two KStreams.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class StreamToStreamJoinIntegrationTest {

  private static final String adImpressionsTopic = "adImpressions";
  private static final String adClicksTopic = "adClicks";
  private static final String outputTopic = "output-topic";

  @Test
  public void shouldJoinTwoStreams() {
    // Input 1: Ad impressions
    final List<TestRecord<String, String>> inputAdImpressions = Arrays.asList(
      new TestRecord<>("car-advertisement", "shown", Instant.ofEpochMilli(10L)), // outer join result
      new TestRecord<>("newspaper-advertisement", "shown", Instant.ofEpochMilli(30L)),
      new TestRecord<>("gadget-advertisement", "shown", Instant.ofEpochMilli(40L))
    );

    // Input 2: Ad clicks
    final List<TestRecord<String, String>> inputAdClicks = Arrays.asList(
      new TestRecord<>("newspaper-advertisement", "clicked", Instant.ofEpochMilli(35L)),
      new TestRecord<>("gadget-advertisement", "clicked", Instant.ofEpochMilli(45L)),
      new TestRecord<>("newspaper-advertisement", "clicked", Instant.ofEpochMilli(30L).plus(Duration.ofSeconds(5L))),
      new TestRecord<>("newspaper-advertisement", "clicked", Instant.ofEpochMilli(31L).plus(Duration.ofSeconds(5L))) // advance stream-time and "drain" outer join result
    );

    final List<TestRecord<String, String>> expectedResults = Arrays.asList(
      new TestRecord<>("newspaper-advertisement", "shown/clicked", Instant.ofEpochMilli(35L)),
      new TestRecord<>("gadget-advertisement", "shown/clicked", Instant.ofEpochMilli(45L)),
      new TestRecord<>("car-advertisement", "shown/not-clicked-yet", Instant.ofEpochMilli(10L)),
      new TestRecord<>("newspaper-advertisement", "shown/clicked", Instant.ofEpochMilli(30L).plus(Duration.ofSeconds(5L)))
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-join-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> alerts = builder.stream(adImpressionsTopic);
    final KStream<String, String> incidents = builder.stream(adClicksTopic);

    // In this example, we opt to perform an OUTER JOIN between the two streams.  We picked this
    // join type to show how the Streams API will send further join updates downstream whenever,
    // for the same join key (e.g. "newspaper-advertisement"), we receive an update from either of
    // the two joined streams during the defined join window.
    final KStream<String, String> impressionsAndClicks = alerts.outerJoin(
      incidents,
      (impressionValue, clickValue) ->
        (clickValue == null)? impressionValue + "/not-clicked-yet": impressionValue + "/" + clickValue,
      // KStream-KStream joins are always windowed joins, hence we must provide a join window.
      JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)),
      // In this specific example, we don't need to define join serdes explicitly because the key, left value, and
      // right value are all of type String, which matches our default serdes configured for the application.  However,
      // we want to showcase the use of `StreamJoined.with(...)` in case your code needs a different type setup.
      StreamJoined.with(
        Serdes.String(), /* key */
        Serdes.String(), /* left value */
        Serdes.String()  /* right value */
      )
    );

    // Write the results to the output topic.
    impressionsAndClicks.to(outputTopic);

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<String, String> impressionInput = topologyTestDriver
        .createInputTopic(adImpressionsTopic,
                          new StringSerializer(),
                          new StringSerializer());
      final TestInputTopic<String, String> clickInput = topologyTestDriver
        .createInputTopic(adClicksTopic,
                          new StringSerializer(),
                          new StringSerializer());
      final TestOutputTopic<String, String> output = topologyTestDriver
        .createOutputTopic(outputTopic, new StringDeserializer(), new StringDeserializer());

      //
      // Step 3: Publish input data.
      //
      impressionInput.pipeRecordList(inputAdImpressions);
      clickInput.pipeRecordList(inputAdClicks);

      //
      // Step 4: Verify the application's output data.
      //
      assertThat(output.readRecordsToList(), equalTo(expectedResults));
    }
  }
}