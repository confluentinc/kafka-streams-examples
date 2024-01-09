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

import io.confluent.examples.streams.avro.PlayEvent;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SessionWindowsExampleTest {

  // A mocked schema registry for our serdes to use
  private static final String SCHEMA_REGISTRY_SCOPE = SessionWindowsExampleTest.class.getName();
  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

  private TopologyTestDriver topologyTestDriver;
  private TestInputTopic<String, PlayEvent> input;
  private TestOutputTopic<String, Long> output;
  private final Map<String, String> AVRO_SERDE_CONFIG = Collections.singletonMap(
    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL
  );

  @Before
  public void createStreams() {
    topologyTestDriver = new TopologyTestDriver(
      SessionWindowsExample.buildTopology(AVRO_SERDE_CONFIG),
      SessionWindowsExample.streamsConfig("dummy", TestUtils.tempDirectory().getPath())
    );
    final SpecificAvroSerializer<PlayEvent> playEventSerializer = new SpecificAvroSerializer<>();
    playEventSerializer.configure(AVRO_SERDE_CONFIG, false);

    input = topologyTestDriver.createInputTopic(SessionWindowsExample.PLAY_EVENTS,
                                                new StringSerializer(),
                                                playEventSerializer);
    output = topologyTestDriver.createOutputTopic(SessionWindowsExample.PLAY_EVENTS_PER_SESSION,
                                                  new StringDeserializer(),
                                                  new LongDeserializer());
  }

  @After
  public void closeStreams() {
    if (topologyTestDriver != null) {
      topologyTestDriver.close();
    }
    MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
  }

  @Test
  public void shouldCountPlayEventsBySession() {
    final long start = System.currentTimeMillis();

    final String userId = "erica";

    input.pipeInput(userId, new PlayEvent(1L, 10L), start);

    final List<KeyValue<String, Long>> firstSession = output.readKeyValuesToList();
    // should have a session for erica with start and end time the same
    assertThat(firstSession.get(0), equalTo(KeyValue.pair(userId + "@" +start+"->"+start, 1L)));

    // also look in the store to find the same session
    final ReadOnlySessionStore<String, Long> playEventsPerSession =
      topologyTestDriver.getSessionStore(SessionWindowsExample.PLAY_EVENTS_PER_SESSION);

    final KeyValue<Windowed<String>, Long> next = fetchSessionsFromLocalStore(userId, playEventsPerSession).get(0);
    assertThat(next.key, equalTo(new Windowed<>(userId, new SessionWindow(start, start))));
    assertThat(next.value, equalTo(1L));

    // send another event that is after the inactivity gap, so we have 2 independent sessions
    final long secondSessionStart = start + SessionWindowsExample.INACTIVITY_GAP.toMillis() + 1;

    input.pipeInput(userId, new PlayEvent(2L, 10L), secondSessionStart);

    final List<KeyValue<String, Long>> secondSession = output.readKeyValuesToList();
    // should have created a new session
    assertThat(secondSession.get(0), equalTo(KeyValue.pair(userId + "@" + secondSessionStart + "->" + secondSessionStart,
                                                           1L)));

    // should now have 2 active sessions in the store
    final List<KeyValue<Windowed<String>, Long>> results = fetchSessionsFromLocalStore(userId, playEventsPerSession);
    assertThat(results, equalTo(Arrays.asList(KeyValue.pair(new Windowed<>(userId, new SessionWindow(start, start)),1L),
                                              KeyValue.pair(new Windowed<>(userId, new SessionWindow(secondSessionStart, secondSessionStart)),1L))));

    // create an event between the two sessions to demonstrate merging
    final long mergeTime = start + SessionWindowsExample.INACTIVITY_GAP.toMillis() / 2;

    input.pipeInput(userId, new PlayEvent(3L, 10L), mergeTime);

    final List<KeyValue<String, Long>> merged = output.readKeyValuesToList();
    // should have merged all sessions into one and sent tombstones for the sessions that were
    // merged
    assertThat(merged, equalTo(Arrays.asList(KeyValue.pair(userId + "@" +start+"->"+start, null),
                                             KeyValue.pair(userId + "@" +secondSessionStart
                                                           +"->"+secondSessionStart, null),
                                             KeyValue.pair(userId + "@"
                                                         +start+"->"+secondSessionStart,
                                                    3L))));

    // should only have the merged session in the store
    final List<KeyValue<Windowed<String>, Long>> mergedResults = fetchSessionsFromLocalStore(userId, playEventsPerSession);
    assertThat(mergedResults, equalTo(Collections.singletonList(KeyValue.pair(new Windowed<>(userId, new SessionWindow(start, secondSessionStart)), 3L))));
  }

  private List<KeyValue<Windowed<String>, Long>> fetchSessionsFromLocalStore(final String userId,
                                                                             final ReadOnlySessionStore<String, Long> playEventsPerSession) {
    final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
    try (final KeyValueIterator<Windowed<String>, Long> iterator = playEventsPerSession.fetch(userId)) {
      iterator.forEachRemaining(results::add);
    }
    return results;
  }
}
