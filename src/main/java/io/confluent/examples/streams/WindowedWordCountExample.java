/*
 * Extended example based on Confluent Kafka Streams examples.
 * Adds windowed aggregation and filtering logic.
 *
 * Licensed under Apache License 2.0
 */

package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WindowedWordCountExample {

    static final String inputTopic = "streams-plaintext-input";
    static final String outputTopic = "streams-wordcount-windowed-output";

    public static void main(final String[] args) {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers);

        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static Properties getStreamsConfiguration(final String bootstrapServers) {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-wordcount-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "windowed-wordcount-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return streamsConfiguration;
    }

    static void createWordCountStream(final StreamsBuilder builder) {

        final KStream<String, String> textLines = builder.stream(inputTopic);

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        /*
         * Processing:
         * 1. Split lines into words
         * 2. Filter small words
         * 3. Group by word
         * 4. Apply tumbling window (10 seconds)
         * 5. Count occurrences
         */
        final KTable<Windowed<String>, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .filter((key, word) -> word.length() > 3)
                .groupBy((keyIgnored, word) -> word)
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count();

        /*
         * Output format:
         * word@windowStartTimestamp -> count
         */
        wordCounts
                .toStream()
                .map((windowedKey, value) ->
                        KeyValue.pair(
                                windowedKey.key() + "@" + windowedKey.window().start(),
                                value
                        )
                )
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
    }
}