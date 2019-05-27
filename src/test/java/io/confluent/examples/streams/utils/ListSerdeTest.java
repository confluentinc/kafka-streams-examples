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
package io.confluent.examples.streams.utils;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ListSerdeTest {

  @Test
  public void shouldRoundTrip() {
    // Given
    final List<String> xs = Arrays.asList("foo", "bar");
    final String anyTopic = "ANY_TOPIC";
    final ListSerde<String> serde = new ListSerde<>(Serdes.String());

    // When
    final byte[] serializedBytes = serde.serializer().serialize(anyTopic, xs);
    final List<String> deserializedPair = serde.deserializer().deserialize(anyTopic, serializedBytes);

    // Then
    assertThat(deserializedPair, equalTo(xs));
  }

  @Test
  public void shouldSupportEmptyList() {
    // Given
    final List<String> xs = Arrays.asList();
    final String anyTopic = "ANY_TOPIC";
    final ListSerde<String> serde = new ListSerde<>(Serdes.String());

    // When
    final byte[] serializedBytes = serde.serializer().serialize(anyTopic, xs);
    final List<String> deserializedPair = serde.deserializer().deserialize(anyTopic, serializedBytes);

    // Then
    assertThat(deserializedPair, equalTo(xs));
  }

  @Test
  public void shouldSupportNullReference() {
    // Given
    final List<String> xs = null;
    final String anyTopic = "ANY_TOPIC";
    final ListSerde<String> serde = new ListSerde<>(Serdes.String());

    // When
    final byte[] serializedBytes = serde.serializer().serialize(anyTopic, xs);
    final List<String> deserializedPair = serde.deserializer().deserialize(anyTopic, serializedBytes);

    // Then
    assertThat(deserializedPair, equalTo(xs));
  }

  @Test
  public void shouldSupportNullElements() {
    // Given
    final List<String> xs = Arrays.asList(null, "foo");
    final String anyTopic = "ANY_TOPIC";
    final ListSerde<String> serde = new ListSerde<>(Serdes.String());

    // When
    final byte[] serializedBytes = serde.serializer().serialize(anyTopic, xs);
    final List<String> deserializedPair = serde.deserializer().deserialize(anyTopic, serializedBytes);

    // Then
    assertThat(deserializedPair, equalTo(xs));
  }

}