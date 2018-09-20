package io.confluent.examples.streams.microservices.util;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.utils.MonitoringInterceptorUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

import static io.confluent.examples.streams.avro.microservices.OrderState.CREATED;
import static io.confluent.examples.streams.avro.microservices.Product.UNDERPANTS;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;

public class ProduceOrders {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        final SpecificAvroSerializer<Order> mySerializer = new SpecificAvroSerializer<>();
        final boolean isKeySerde = false;
        mySerializer.configure(
            Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"),
            isKeySerde);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        MonitoringInterceptorUtils.maybeConfigureInterceptorsProducer(props);

        try (final KafkaProducer<String, Order> producer = new KafkaProducer<>(props, new StringSerializer(), mySerializer)) {
            while (true) {
                final String orderId = id(0L);
                final Order order = new Order(orderId, 15L, CREATED, UNDERPANTS, 3, 5.00d);
                final ProducerRecord<String, Order> record = new ProducerRecord<>("orders", order.getId(), order);
                producer.send(record);
                Thread.sleep(1000L);
            }
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

}

