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

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ListSerializer<T> implements Serializer<List<T>> {

  private final Serializer<T> serializer;

  public ListSerializer(final Serializer<T> serializer) {
    this.serializer = serializer;
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
  }

  @Override
  public byte[] serialize(final String topic, final List<T> xs) {
    if (xs == null) {
      return null;
    }

    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         final DataOutputStream out = new DataOutputStream(baos)) {
      out.writeInt(xs.size());
      for (T x : xs) {
        final byte[] serialized = serializer.serialize(topic, x);
        writeElement(out, serialized);
      }
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("unable to serialize List", e);
    }
  }

  private void writeElement(final DataOutputStream out, final byte[] serialized) throws IOException {
    if (serialized != null) {
      out.writeInt(serialized.length);
      out.write(serialized);
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void close() {
  }

}