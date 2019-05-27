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

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ListDeserializer<T> implements Deserializer<List<T>> {

  private final Deserializer<T> deserializer;

  public ListDeserializer(final Deserializer<T> deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
  }

  @Override
  public List<T> deserialize(final String topic, final byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    final List<T> xs = new LinkedList<>();
    try (final DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      final int numElements = in.readInt();
      for (int i = 0; i < numElements; i++) {
        xs.add(readElement(in, topic));
      }
    } catch (final IOException e) {
      throw new RuntimeException("Unable to deserialize List", e);
    }
    return xs;
  }

  private T readElement(final DataInputStream in, final String topic) throws IOException {
    final int length = in.readInt();
    if (length > 0) {
      final byte[] serialized = new byte[length];
      in.readFully(serialized);
      return deserializer.deserialize(topic, serialized);
    } else {
      return null;
    }
  }

  @Override
  public void close() {
  }

}