package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.utils.MonitoringInterceptorUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static io.confluent.examples.streams.avro.microservices.Product.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.Product.UNDERPANTS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.ProductTypeSerde;
import static java.util.Arrays.asList;

public class AddInventory {

    private static void sendInventory(final List<KeyValue<Product, Integer>> inventory,
                                      final Schemas.Topic<Product, Integer> topic,
                                      final String bootstrapServers) {

        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "inventory-generator");
        MonitoringInterceptorUtils.maybeConfigureInterceptorsProducer(producerConfig);
        final ProductTypeSerde productSerde = new ProductTypeSerde();

        try (final KafkaProducer<Product, Integer> stockProducer = new KafkaProducer<>(
            producerConfig,
            productSerde.serializer(),
            Serdes.Integer().serializer())) {
            for (final KeyValue<Product, Integer> kv : inventory) {
                stockProducer.send(new ProducerRecord<>(topic.name(), kv.key, kv.value))
                             .get();
            }
        } catch (final InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(final String[] args) {

        final int quantityUnderpants = args.length > 0 ? Integer.parseInt(args[0]) : 20;
        final int quantityJumpers = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        final String bootstrapServers = args.length > 2 ? args[2] : "localhost:9092";

        // Send Inventory
        final List<KeyValue<Product, Integer>> inventory = asList(
            new KeyValue<>(UNDERPANTS, quantityUnderpants),
            new KeyValue<>(JUMPERS, quantityJumpers)
        );
        System.out.printf("Send inventory to %s%n", Topics.WAREHOUSE_INVENTORY);
        sendInventory(inventory, Topics.WAREHOUSE_INVENTORY, bootstrapServers);

    }

}
