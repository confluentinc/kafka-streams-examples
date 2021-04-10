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

import io.confluent.examples.streams.avro.Customer;
import io.confluent.examples.streams.avro.EnrichedOrder;
import io.confluent.examples.streams.avro.Order;
import io.confluent.examples.streams.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Demonstrates how to perform "joins" between  KStreams and GlobalStore, i.e. joins that
 * don't require re-partitioning of the input streams.
 * <p>
 * The {@link GlobalKTablesExample} shows another way to perform the same operation using
 * {@link org.apache.kafka.streams.kstream.GlobalKTable} and the join operator.
 * <p>
 * In this example, we join a stream of orders that reads from a topic named
 * "order" with a customers global store that reads from a topic named "customer", and a products
 * global store that reads from a topic "product". The join produces an EnrichedOrder object.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic order \
 *                    --zookeeper localhost:2181 --partitions 4 --replication-factor 1
 * $ bin/kafka-topics --create --topic customer \
 *                    --zookeeper localhost:2181 --partitions 3 --replication-factor 1
 * $ bin/kafka-topics --create --topic product \
 *                    --zookeeper localhost:2181 --partitions 2 --replication-factor 1
 * $ bin/kafka-topics --create --topic enriched-order \
 *                    --zookeeper localhost:2181 --partitions 4 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
 * `bin/kafka-topics.sh ...`.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-6.0.1-standalone.jar io.confluent.examples.streams.GlobalStoresExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link GlobalKTablesAndStoresExampleDriver}). The
 * already running example application (step 3) will automatically process this input data and write
 * the results to the output topic.
 * <pre>
 * {@code
 * # Here: Write input data using the example driver. The driver will exit once it has received
 * # all EnrichedOrders
 * $ java -cp target/kafka-streams-examples-6.0.1-standalone.jar io.confluent.examples.streams.GlobalKTablesAndStoresExampleDriver
 * }
 * </pre>
 * <p>
 * 5) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Confluent Schema Registry ({@code Ctrl-C}), then stop the Kafka broker ({@code Ctrl-C}), and
 * only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class GlobalStoresExample {

    static final String ORDER_TOPIC = "order";
    static final String CUSTOMER_TOPIC = "customer";
    static final String PRODUCT_TOPIC = "product";
    static final String CUSTOMER_STORE = "customer-store";
    static final String PRODUCT_STORE = "product-store";
    static final String ENRICHED_ORDER_TOPIC = "enriched-order";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final KafkaStreams
                streams =
                createStreams(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams-global-stores");
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        // start processing
        streams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static KafkaStreams createStreams(final String bootstrapServers,
                                             final String schemaRegistryUrl,
                                             final String stateDir) {

        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-stores-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "global-stores-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        // Set to earliest so we don't miss any data that arrived in the topics before the process
        // started
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create and configure the SpecificAvroSerdes required in this example
        final SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig =
                Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl);
        orderSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<EnrichedOrder> enrichedOrdersSerde = new SpecificAvroSerde<>();
        enrichedOrdersSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();

        // Get the stream of orders
        final KStream<Long, Order> ordersStream = builder.stream(ORDER_TOPIC, Consumed.with(Serdes.Long(), orderSerde));

        // Add a global store for customers. The data from this global store
        // will be fully replicated on each instance of this application.
        builder.addGlobalStore(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(CUSTOMER_STORE), Serdes.Long(), customerSerde),
                CUSTOMER_TOPIC,
                Consumed.with(Serdes.Long(), customerSerde),
                () -> new GlobalStoreUpdater<>(CUSTOMER_STORE));

        // Add a global store for products. The data from this global store
        // will be fully replicated on each instance of this application.
        builder.addGlobalStore(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(PRODUCT_STORE), Serdes.Long(), productSerde),
                PRODUCT_TOPIC,
                Consumed.with(Serdes.Long(), productSerde),
                () -> new GlobalStoreUpdater<>(PRODUCT_STORE));

        // We transform each order with a value transformer which will access each
        // global store to retrieve customer and product linked to the order.
        final KStream<Long, EnrichedOrder> enrichedOrdersStream = ordersStream.transformValues(() -> new ValueTransformer<Order, EnrichedOrder>() {

            private KeyValueStore<Long, Customer> customerStore;
            private KeyValueStore<Long, Product> productStore;

            @Override
            public void init(final org.apache.kafka.streams.processor.ProcessorContext processorContext) {
                customerStore = processorContext.getStateStore(CUSTOMER_STORE);
                productStore = processorContext.getStateStore(PRODUCT_STORE);
            }

            @Override
            public EnrichedOrder transform(final Order order) {
                // Get the customer corresponding the order from the customer store
                final Customer customer = customerStore.get(order.getCustomerId());

                // Get the product corresponding the order from the product store
                final Product product = productStore.get(order.getProductId());

                return new EnrichedOrder(product, customer, order);
            }

            @Override
            public void close() {
                // No-op
            }
        });

        // write the enriched order to the enriched-order topic
        enrichedOrdersStream.to(ENRICHED_ORDER_TOPIC, Produced.with(Serdes.Long(), enrichedOrdersSerde));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    // Processor that keeps the global store updated.
    private static class GlobalStoreUpdater<K, V> implements Processor<K, V, Void, Void> {

        private final String storeName;

        public GlobalStoreUpdater(final String storeName) {
            this.storeName = storeName;
        }

        private KeyValueStore<K, V> store;

        @Override
        public void init(final ProcessorContext<Void, Void> processorContext) {
            store = processorContext.getStateStore(storeName);
        }

        @Override
        public void process(final Record<K, V> record) {
            // We are only supposed to put operation the keep the store updated.
            // We should not filter record or modify the key or value
            // Doing so would break fault-tolerance.
            // see https://issues.apache.org/jira/browse/KAFKA-7663
            store.put(record.key(), record.value());
        }

        @Override
        public void close() {
            // No-op
        }

    }
}
