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
package io.confluent.examples.streams.window;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;


public class CustomWindowTest {

    private static final String inputTopic = "inputTopic";
    private static final String outputTopic = "outputTopic";
    private static final ZoneId zone = ZoneOffset.UTC;
    private static final int windowStartHour = 18;

    @Test
    public void shouldSumNumbersOnSameDay() {
      final List<TestRecord<String, Integer>> inputValues = Arrays.asList(
        new TestRecord<>(null,
                         1,
                         ZonedDateTime.of(2019, 1, 1, 16, 29, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         2,
                         ZonedDateTime.of(2019, 1, 1, 16, 30, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         7,
                         ZonedDateTime.of(2019, 1, 1, 16, 31, 0, 0, zone).toInstant()),
        dummyEventToForceSuppression()
        );
      final List<KeyValue<Windowed<Integer>, Integer>> expectedValues =
        Collections.singletonList(KeyValue.pair(toWindowed(1,
                                                           ZonedDateTime.of(2018, 12, 31, 18, 0, 0, 0, zone),
                                                           ZonedDateTime.of(2019, 1, 1, 18, 0, 0, 0, zone)),
                                                10)
      );
      verify(inputValues, expectedValues, zone);
    }

    @Test
    public void shouldSumNumbersWithTwoWindows() {
      final List<TestRecord<String, Integer>> inputValues = Arrays.asList(
        new TestRecord<>(null,
                         1,
                         ZonedDateTime.of(2019, 1, 1, 16, 29, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         2,
                         ZonedDateTime.of(2019, 1, 1, 16, 30, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         7,
                         ZonedDateTime.of(2019, 1, 1, 18, 31, 0, 0, zone).toInstant()),
        dummyEventToForceSuppression()
      );
      final List<KeyValue<Windowed<Integer>, Integer>> expectedValues = Arrays.asList(
        KeyValue.pair(toWindowed(1,
                                 ZonedDateTime.of(2018, 12, 31, 18, 0, 0, 0, zone),
                                 ZonedDateTime.of(2019, 1, 1, 18, 0, 0, 0, zone)),
                      3),
        KeyValue.pair(toWindowed(1,
                                 ZonedDateTime.of(2019, 1, 1, 18, 0, 0, 0, zone),
                                 ZonedDateTime.of(2019, 1, 2, 18, 0, 0, 0, zone)),
                      7)
      );
      verify(inputValues, expectedValues, zone);
    }

    @Test
    public void shouldSumNumbersWithTwoWindowsAndLateArrival() {
      final List<TestRecord<String, Integer>> inputValues = Arrays.asList(
        new TestRecord<>(null,
                         1,
                         ZonedDateTime.of(2019, 1, 1, 16, 29, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         2,
                         ZonedDateTime.of(2019, 1, 1, 16, 30, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         2,
                         ZonedDateTime.of(2019, 1, 1, 18, 1, 0, 0, zone).toInstant()),
        //Out-of-order arrival message
        new TestRecord<>(null,
                         7, ZonedDateTime.of(2019, 1, 1, 16, 31, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         40,
                         ZonedDateTime.of(2019, 1, 1, 18, 31, 0, 0, zone).toInstant()),
        //this late arrival event should be ignored as it happens after a message that was outside of grace period 18h (end of window) + 30min (grace period)
        new TestRecord<>(null,
                         42,
                         ZonedDateTime.of(2019, 1, 1, 16, 35, 0, 0, zone).toInstant()),
        dummyEventToForceSuppression()
      );

      final List<KeyValue<Windowed<Integer>, Integer>> expectedValues = Arrays.asList(
        KeyValue.pair(toWindowed(1,
                                 ZonedDateTime.of(2018, 12, 31, 18, 0, 0, 0, zone),
                                 ZonedDateTime.of(2019, 1, 1, 18, 0, 0, 0, zone)),
                      10),
        KeyValue.pair(toWindowed(1,
                                 ZonedDateTime.of(2019, 1, 1, 18, 0, 0, 0, zone),
                                 ZonedDateTime.of(2019, 1, 2, 18, 0, 0, 0, zone)),
                      42)
      );
      verify(inputValues, expectedValues, zone);
    }

    // Daylight savings time tests

    @Test
    public void shouldSumNumbersWithTwoWindowsAndNoDSTTimezone() {
      final List<TestRecord<String, Integer>> inputValues = Arrays.asList(
        new TestRecord<>(null,
                         1,
                         ZonedDateTime.of(2019, 3, 30, 1, 39, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         2,
                         ZonedDateTime.of(2019, 3, 30, 2, 0, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         7,
                         ZonedDateTime.of(2019, 3, 30, 2, 10, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         1,
                         ZonedDateTime.of(2019, 3, 31, 1, 39, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         2,
                         ZonedDateTime.of(2019, 3, 31, 2, 0, 0, 0, zone).toInstant()),
        new TestRecord<>(null,
                         7,
                         ZonedDateTime.of(2019, 3, 31, 2, 10, 0, 0, zone).toInstant()),
        dummyEventToForceSuppression()
      );
      final List<KeyValue<Windowed<Integer>, Integer>> expectedValues = Arrays.asList(
        KeyValue.pair(toWindowed(1,
                                 ZonedDateTime.of(2019, 3, 29, 18, 0, 0, 0, zone),
                                 ZonedDateTime.of(2019, 3, 30, 18, 0, 0, 0, zone)),
                      10),
        KeyValue.pair(toWindowed(1,
                                 ZonedDateTime.of(2019, 3, 30, 18, 0, 0, 0, zone),
                                 ZonedDateTime.of(2019, 3, 31, 18, 0, 0, 0, zone)),
                      10)
      );
      verify(inputValues, expectedValues, zone);
    }

    @Test
    public void shouldSumNumbersWithTwoWindowsAndDSTTimezone() {
      //This test illustrate problems with daylight savings
      //Some timezone have daylight savings time (DST) resulting in two days in year that have either 23 or 25 hours.
      //Kafka streams currently support only fixed period for the moment.
      final ZoneId zoneWithDST = ZoneId.of("Europe/Paris");

      final List<TestRecord<String, Integer>> inputValues = Arrays.asList(
        new TestRecord<>(null,
                         1,
                         ZonedDateTime.of(2019, 3, 30, 1, 39, 0, 0, zoneWithDST).toInstant()),
        new TestRecord<>(null,
                         2,
                         ZonedDateTime.of(2019, 3, 30, 2, 0, 0, 0, zoneWithDST).toInstant()),
        new TestRecord<>(null,
                         7,
                         ZonedDateTime.of(2019, 3, 30, 2, 10, 0, 0, zoneWithDST).toInstant()),
        new TestRecord<>(null,
                         1,
                         ZonedDateTime.of(2019, 3, 31, 1, 39, 0, 0, zoneWithDST).toInstant()),
        new TestRecord<>(null,
                         2,
                         ZonedDateTime.of(2019, 3, 31, 2, 0, 0, 0, zoneWithDST).toInstant()),
        new TestRecord<>(null,
                         7,
                         ZonedDateTime.of(2019, 3, 31, 2, 10, 0, 0, zoneWithDST).toInstant()),
        dummyEventToForceSuppression()
      );
      final List<KeyValue<Windowed<Integer>, Integer>> expectedValues = Arrays.asList(
        KeyValue.pair(toWindowed(1,
                                 ZonedDateTime.of(2019, 3, 29, 18, 0, 0, 0, zoneWithDST),
                                 ZonedDateTime.of(2019, 3, 30, 18, 0, 0, 0, zoneWithDST)),
                      10),
        KeyValue.pair(toWindowed(1,
                                 ZonedDateTime.of(2019, 3, 30, 18, 0, 0, 0, zoneWithDST),
                                 //This get one extra hour due to time shift in Daylight saving
                                 //As a user i would expect it to end on 31th at 6pm.

                                 //The limitation seems to come from TimeWindowSerializer /
                                 // TimeWindowDeserializer as we serialize only start date.
                                 // Suggestion: By serializing both start and end of window,
                                 // we could support more complex cases with non fixed time
                                 // window and address daylight savings on daily windows.
                                 ZonedDateTime.of(2019, 3, 31, 19, 0, 0, 0, zoneWithDST)),
                      10)
      );
      verify(inputValues, expectedValues, zoneWithDST);
    }

    private void verify(final List<TestRecord<String, Integer>> inputValues,
                        final List<KeyValue<Windowed<Integer>, Integer>> expectedValues,
                        final ZoneId zoneId) {

      final Properties streamsConfiguration = new Properties();
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-integration-test");
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
      streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
      streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

      final Topology topology = buildKafkaStreamTopology(zoneId);

      try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamsConfiguration)) {
        final TestInputTopic<String, Integer> input = testDriver
          .createInputTopic(inputTopic,
                            new StringSerializer(),
                            new IntegerSerializer());
        final TestOutputTopic<Windowed<Integer>, Integer> output = testDriver
          .createOutputTopic(outputTopic,
                             new TimeWindowedDeserializer<>(
                               new IntegerDeserializer(),
                               Duration.ofDays(1).toMillis()),
                             new IntegerDeserializer());

        input.pipeRecordList(inputValues);
        assertThat(output.readKeyValuesToList(), equalTo(expectedValues));
      }
    }

    private Topology buildKafkaStreamTopology(final ZoneId zoneId) {
      final StreamsBuilder builder = new StreamsBuilder();

      final KStream<String, Integer> input = builder.stream(inputTopic);

      final Duration gracePeriod = Duration.ofMinutes(30L);

      final KStream<Windowed<Integer>, Integer> sumOfOddNumbers = input
        .selectKey((k, v) -> 1)
        .groupByKey()
        .windowedBy(new DailyTimeWindows(zoneId, windowStartHour, gracePeriod))
        // A simple sum of value
        .reduce(Integer::sum,
                Materialized.<Integer, Integer, WindowStore<Bytes, byte[]>>with(Serdes.Integer(),
                                                                                Serdes.Integer())
                  // the default store retention time is 1 day;
                  // need to explicitly increase the retention time
                  // to allow for a 1-day window plus configured grace period
                  .withRetention(Duration.ofDays(1L).plus(gracePeriod)))
        // We only care about final result
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream();
      sumOfOddNumbers.print(Printed.toSysOut());
      sumOfOddNumbers.to(outputTopic, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Integer.class), Serdes.Integer()));
      return builder.build();
    }

    private Windowed<Integer> toWindowed(final Integer key,
                                         final ZonedDateTime start,
                                         final ZonedDateTime end) {
      return new Windowed<>(key, new TimeWindow(start.toInstant().toEpochMilli(),
                                                end.toInstant().toEpochMilli()));
    }

    /** Generates an event after window end + grace period to trigger flush everything through suppression
     @see KTable#suppress(Suppressed)
    */
    private TestRecord<String, Integer> dummyEventToForceSuppression() {
      return new TestRecord<>(null, 7, ZonedDateTime.now().toInstant());
    }
}
