/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.examples.streams.utils;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import java.util.Comparator;
import java.util.PriorityQueue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PriorityQueueSerdeTest {

    @Test
    public void shouldSerializeDeserialize() throws Exception {
        final Comparator<Long> comparator = Long::compareTo;
        final PriorityQueue<Long> queue = new PriorityQueue<>(comparator);
        queue.add(2L);
        queue.add(1L);

        final PriorityQueueSerde<Long> serde = new PriorityQueueSerde<>(comparator, Serdes.Long());
        final byte[] bytes = serde.serializer().serialize("t", queue);
        final PriorityQueue<Long> result = serde.deserializer().deserialize("t", bytes);
        assertThat(result.poll(), equalTo(1L));
        assertThat(result.poll(), equalTo(2L));
        assertThat(result.poll(), nullValue());
    }

}