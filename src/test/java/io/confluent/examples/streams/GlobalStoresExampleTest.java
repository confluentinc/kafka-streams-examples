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

import org.apache.kafka.streams.KafkaStreams;

/**
 * Test class for {@link GlobalKTablesExample}. See {@link AbstractGlobalStoresAndTablesTest}.
 */
public class GlobalStoresExampleTest extends AbstractGlobalStoresAndTablesTest {

    @Override
    public KafkaStreams createStreamsInstance(final String bootstrapServers, final String schemaRegistryUrl, final String tempDirectoryPath) {
        return GlobalStoresExample.createStreams(bootstrapServers, schemaRegistryUrl, tempDirectoryPath);
    }

    @Override
    public String getGroupId() {
        return "global-stores-consumer";
    }
}
