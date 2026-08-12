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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Supplier;

/**
 * Test helpers adapted from {@code org.apache.kafka.test.TestUtils} in Apache Kafka.
 *
 * <p>Those helpers moved from the {@code clients} test source set to test fixtures in Apache
 * Kafka PR #22201, so they are no longer present in the published {@code kafka-clients}
 * {@code test} artifact this project resolves. Only the helpers this project actually uses are
 * reproduced here, which also removes the dependency on an internal, non-public Apache Kafka
 * test package.
 *
 * <p>Timeouts and polling intervals match the upstream defaults so test timing is unchanged.
 */
public final class TestUtils {

  public static final long DEFAULT_POLL_INTERVAL_MS = 100;
  public static final long DEFAULT_MAX_WAIT_MS = 15000;

  private static final String DEFAULT_PREFIX = "kafka-";

  private TestUtils() {
  }

  /**
   * Creates a temporary relative directory in the default temporary-file directory with a prefix
   * of {@code kafka-}. The directory is deleted on JVM exit.
   */
  public static File tempDirectory() {
    return tempDirectory(null, null);
  }

  /**
   * Creates a temporary relative directory in the default temporary-file directory with the given
   * prefix, or {@code kafka-} when {@code prefix} is null.
   */
  public static File tempDirectory(final String prefix) {
    return tempDirectory(null, prefix);
  }

  /**
   * Creates a temporary relative directory in the given parent directory, or in the default
   * temporary-file directory when {@code parent} is null.
   */
  public static File tempDirectory(final Path parent, final String prefix) {
    final String actualPrefix = prefix == null ? DEFAULT_PREFIX : prefix;
    final File file;
    try {
      file = parent == null
          ? Files.createTempDirectory(actualPrefix).toFile()
          : Files.createTempDirectory(parent, actualPrefix).toFile();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to create a temp dir", e);
    }

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> deleteRecursively(file), "delete-temp-file-shutdown-hook"));

    return file;
  }

  /**
   * Waits at most {@link #DEFAULT_MAX_WAIT_MS} for the condition to be met, throwing
   * {@link AssertionError} otherwise.
   */
  public static void waitForCondition(final TestCondition testCondition,
                                      final String conditionDetails) throws InterruptedException {
    waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, () -> conditionDetails);
  }

  /**
   * Waits at most {@link #DEFAULT_MAX_WAIT_MS} for the condition to be met, throwing
   * {@link AssertionError} otherwise.
   */
  public static void waitForCondition(final TestCondition testCondition,
                                      final Supplier<String> conditionDetailsSupplier)
      throws InterruptedException {
    waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, conditionDetailsSupplier);
  }

  /**
   * Waits at most {@code maxWaitMs} for the condition to be met, throwing {@link AssertionError}
   * otherwise.
   */
  public static void waitForCondition(final TestCondition testCondition,
                                      final long maxWaitMs,
                                      final String conditionDetails) throws InterruptedException {
    waitForCondition(testCondition, maxWaitMs, () -> conditionDetails);
  }

  /**
   * Waits at most {@code maxWaitMs} for the condition to be met, throwing {@link AssertionError}
   * otherwise.
   */
  public static void waitForCondition(final TestCondition testCondition,
                                      final long maxWaitMs,
                                      final Supplier<String> conditionDetailsSupplier)
      throws InterruptedException {
    waitForCondition(testCondition, maxWaitMs, DEFAULT_POLL_INTERVAL_MS, conditionDetailsSupplier);
  }

  /**
   * Waits at most {@code maxWaitMs} for the condition to be met, polling every
   * {@code pollIntervalMs}, and throws {@link AssertionError} otherwise. Prefer this over
   * {@code Thread.sleep} so that a generous timeout does not lengthen a passing test.
   */
  public static void waitForCondition(final TestCondition testCondition,
                                      final long maxWaitMs,
                                      final long pollIntervalMs,
                                      final Supplier<String> conditionDetailsSupplier)
      throws InterruptedException {
    retryOnExceptionWithTimeout(maxWaitMs, pollIntervalMs, () -> {
      if (!testCondition.conditionMet()) {
        final String supplied =
            conditionDetailsSupplier == null ? null : conditionDetailsSupplier.get();
        throw new AssertionError(
            "Condition not met within timeout " + maxWaitMs + ". " + (supplied == null ? "" : supplied));
      }
    });
  }

  /**
   * Retries {@code runnable} until it completes without throwing, or until {@code timeoutMs}
   * elapses, in which case the last failure is rethrown so it provides context.
   */
  public static void retryOnExceptionWithTimeout(final long timeoutMs,
                                                 final long pollIntervalMs,
                                                 final ValuelessCallable runnable)
      throws InterruptedException {
    final long expectedEnd = System.currentTimeMillis() + timeoutMs;

    while (true) {
      try {
        runnable.call();
        return;
      } catch (final AssertionError t) {
        if (expectedEnd <= System.currentTimeMillis()) {
          throw t;
        }
      } catch (final Exception e) {
        if (expectedEnd <= System.currentTimeMillis()) {
          throw new AssertionError(
              String.format("Assertion failed with an exception after %s ms", timeoutMs), e);
        }
      }
      Thread.sleep(Math.min(pollIntervalMs, timeoutMs));
    }
  }

  private static void deleteRecursively(final File file) {
    if (!file.exists()) {
      return;
    }
    try {
      Files.walkFileTree(file.toPath(), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(final Path path,
                                         final BasicFileAttributes attrs) throws IOException {
          Files.deleteIfExists(path);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir,
                                                  final IOException exc) throws IOException {
          Files.deleteIfExists(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (final IOException e) {
      // Best effort only: this runs on JVM shutdown, where the test result is already decided.
    }
  }

  /**
   * A {@link Runnable} that is allowed to throw, used by
   * {@link #retryOnExceptionWithTimeout(long, long, ValuelessCallable)}.
   */
  @FunctionalInterface
  public interface ValuelessCallable {
    void call() throws Exception;
  }
}
