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

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful
 * computation, using the generic Avro binding for serdes in Kafka Streams.  Same as
 * PageViewRegionExample but uses lambda expressions and thus only works on Java 8+.
 * <p>
 * In this example, we join a stream of page views (aka clickstreams) that reads from a topic named
 * "PageViews" with a user profile table that reads from a topic named "UserProfiles" to compute the
 * number of page views per user region.
 * <p>
 * Note: The generic Avro binding is used for serialization/deserialization. This means the
 * appropriate Avro schema files must be provided for each of the "intermediate" Avro classes, i.e.
 * whenever new types of Avro objects (in the form of GenericRecord) are being passed between
 * processing steps.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 *
 * docker-compose exec broker kafka-topics --create --topic LoanApplication \
 *                  --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1
 * docker-compose exec broker kafka-topics --create --topic Company \
 *                  --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1
 * docker-compose exec broker kafka-topics --create --topic LoanApplicationEnriched \
 *                  --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1
 *
 * }
 * </pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-5.3.0-standalone.jar io.confluent.examples.streams.PageViewRegionLambdaExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link PageViewRegionExampleDriver}).
 * The already running example application (step 3) will automatically process this input data and
 * write the results to the output topic.
 * <pre>
 * {@code
 * # Here: Write input data using the example driver. Once the driver has stopped generating data,
 * # you can terminate it via `Ctrl-C`.
 * $ java -cp target/kafka-streams-examples-5.3.0-standalone.jar io.confluent.examples.streams.PageViewRegionExampleDriver
 * }
 * </pre>
 * 5) Inspect the resulting data in the output topic, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic LoanApplicationEnriched --from-beginning \
 *                              --bootstrap-server localhost:9092 \
 *                              --property print.key=true \
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }
 * </pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 * [africa@1466515140000]  2
 * [asia@1466514900000]  3
 * ...
 * }
 * </pre>
 * Here, the output format is "[REGION@WINDOW_START_TIME] COUNT".
 * <p>
 * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Confluent Schema Registry ({@code Ctrl-C}), then stop the Kafka broker ({@code Ctrl-C}), and
 * only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class LoanApplicationCompanyLambdaExample {

    public static void main(final String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "loanapplication-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "loanapplication-lambda-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, GenericRecord> views = builder.stream("LoanApplication");

        // Create a keyed stream of page view events from the PageViews stream,
        // by extracting the user id (String) from the Avro value
        final KStream<String, GenericRecord> loanAppByExternalCompanyId = views
                .selectKey((k, v) -> v.get("company-id").toString());

        final KTable<String, GenericRecord> companies = builder.table("Company");

        // Create a changelog stream as a projection of the value to the region attribute only
        final KTable companyExternalId =
                companies
                        .groupBy((internalCompanyId, company) -> KeyValue.pair(company.get("external-id").toString(), company))
                        .aggregate(
                                // Initiate the aggregate value
                                () -> null,
                                // adder (doing nothing, just passing the user through as the value)
                                (externalId, company, aggValue) -> company,
                                // subtractor (doing nothing, just passing the user through as the value)
                                (externalId, company, aggValue) -> company
                        );

        // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
        // In this example, we want to create an intermediate GenericRecord to hold the view region.
        // See `pageviewregion.avsc` under `src/main/avro/`.
        final InputStream
                loanApplicationEnrichedSchema =
                LoanApplicationCompanyLambdaExample.class.getClassLoader()
                        .getResourceAsStream("avro/io/confluent/examples/streams/loan-application-enriched.avsc");
        final Schema schema = new Schema.Parser().parse(loanApplicationEnrichedSchema);

        final KTable<Windowed<String>, Long> loanApplicationEnriched = loanAppByExternalCompanyId
                .leftJoin(companyExternalId, (loanApp, Company) -> {
                    final GenericRecord viewRegion = new GenericData.Record(schema);
                    viewRegion.put("user", view.get("user"));
                    viewRegion.put("page", view.get("page"));
                    viewRegion.put("region", region);
                    return viewRegion;
                })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").toString(), viewRegion))
                // count views by region, using hopping windows of size 5 minutes that advance every 1 minute
                .groupByKey() // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
                .count();

        // Note: The following operations would NOT be needed for the actual pageview-by-region
        // computation, which would normally stop at `count` above.  We use the operations
        // below only to "massage" the output data so it is easier to inspect on the console via
        // kafka-console-consumer.
        final KStream<String, Long> viewsByRegionForConsole = loanApplicationEnriched
                // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
                // and by also converting the record key from type `Windowed<String>` (which
                // kafka-console-consumer can't print to console out-of-the-box) to `String`
                .toStream((windowedRegion, count) -> windowedRegion.toString());

        viewsByRegionForConsole.to("PageViewsByRegion", Produced.with(stringSerde, longSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
