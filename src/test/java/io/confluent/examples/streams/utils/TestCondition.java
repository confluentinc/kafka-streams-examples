/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

/**
 * Interface to wrap actions that are required to wait until a condition is met for testing
 * purposes. Note that this is not intended to do any assertions.
 *
 * <p>Adapted from {@code org.apache.kafka.test.TestCondition} in Apache Kafka. That class moved
 * from the {@code clients} test source set to test fixtures in Apache Kafka PR #22201, and is
 * therefore no longer present in the published {@code kafka-clients} {@code test} artifact this
 * project resolves.
 */
@FunctionalInterface
public interface TestCondition {

  boolean conditionMet() throws Exception;
}
