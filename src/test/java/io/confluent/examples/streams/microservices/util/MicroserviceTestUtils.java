package io.confluent.examples.streams.microservices.util;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.domain.Schemas;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.ClassRule;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class MicroserviceTestUtils {
    private static int consumerCounter = 0;

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster(MicroserviceTestUtils.propsWith(
            //Transactions need durability so the defaults require multiple nodes.
            //For testing purposes set transactions to work with a single kafka broker.
            new KeyValue<>(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1"),
            new KeyValue<>(KafkaConfig.TransactionsTopicMinISRProp(), "1"),
            new KeyValue<>(KafkaConfig.TransactionsTopicPartitionsProp(), "1")
    ));

    protected static Properties producerConfig(EmbeddedSingleNodeKafkaCluster cluster) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        return producerConfig;
    }

    public static <K, V> List<V> read(Schemas.Topic<K, V> topic, int numberToRead, String bootstrapServers) throws InterruptedException {
        return readKeyValues(topic, numberToRead, bootstrapServers).stream().map(kv -> kv.value).collect(Collectors.toList());
    }

    public static <K, V> List<K> readKeys(Schemas.Topic<K, V> topic, int numberToRead, String bootstrapServers) throws InterruptedException {
        return readKeyValues(topic, numberToRead, bootstrapServers).stream().map(kv -> kv.key).collect(Collectors.toList());
    }

    public static <K, V> List<KeyValue<K, V>> readKeyValues(Schemas.Topic<K, V> topic, int numberToRead, String bootstrapServers) throws InterruptedException {
        Deserializer<K> keyDes = topic.keySerde().deserializer();
        Deserializer<V> valDes = topic.valueSerde().deserializer();
        String topicName = topic.name();
        return readKeysAndValues(numberToRead, bootstrapServers, keyDes, valDes, topicName);
    }

    private static <K, V> List<KeyValue<K, V>> readKeysAndValues(int numberToRead, String bootstrapServers, Deserializer<K> keyDes, Deserializer<V> valDes, String topicName) throws InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "Test-Reader-" + consumerCounter++);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig, keyDes, valDes);
        consumer.subscribe(singletonList(topicName));

        List<KeyValue<K, V>> actualValues = new ArrayList<>();
        org.apache.kafka.test.TestUtils.waitForCondition(() -> {
            ConsumerRecords<K, V> records = consumer.poll(100);
            for (ConsumerRecord<K, V> record : records) {
                actualValues.add(KeyValue.pair(record.key(), record.value()));
            }
            return actualValues.size() == numberToRead;
        }, 20000, "Timed out reading orders.");
        consumer.close();
        return actualValues;
    }

    private static List<TopicTailer> tailers = new ArrayList<>();

    private static <K, V> void tailAllTopicsToConsole(Schemas.Topic<K, V> topic, String bootstrapServers) {
        TopicTailer task = new TopicTailer<>(topic, bootstrapServers);
        tailers.add(task);
        Executors.newSingleThreadExecutor().execute(task);
    }

    public static void stopTailers() {
        tailers.forEach(TopicTailer::stop);
    }

    public static void tailAllTopicsToConsole(String bootstrapServers) {
        for (Schemas.Topic t : Schemas.Topics.ALL.values()) {
            tailAllTopicsToConsole(t, bootstrapServers);
        }
    }

    static class TopicTailer<K, V> implements Runnable {
        private SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        private boolean running = true;
        private boolean closed = false;
        private Schemas.Topic<K, V> topic;
        private String bootstrapServers;

        public TopicTailer(Schemas.Topic<K, V> topic, String bootstrapServers) {
            this.topic = topic;
            this.bootstrapServers = bootstrapServers;
        }

        @Override
        public void run() {
            try {
                Properties consumerConfig = new Properties();
                consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "Test-Reader-" + consumerCounter++);
                consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig, topic.keySerde().deserializer(), topic.valueSerde().deserializer());
                consumer.subscribe(singletonList(topic.name()));

                while (running) {
                    ConsumerRecords<K, V> records = consumer.poll(100);
                    for (ConsumerRecord<K, V> record : records) {
                        System.out.println("[" + format.format(new Date(record.timestamp())) + "]-[" + topic.name() + ":" + record.offset() + "]: " + "->" + record.value());
                    }
                }
                consumer.close();
            } finally {
                closed = true;
            }
        }

        void stop() {
            running = false;
            while (!closed) try {
                Thread.sleep(200);
                System.out.println("Closing tailer...");
            } catch (InterruptedException e) {
            }
        }

    }

    public static Properties propsWith(KeyValue... props) {
        Properties properties = new Properties();
        for (KeyValue kv : props) {
            properties.put(kv.key, kv.value);
        }
        return properties;
    }

    //TODO refactor these send messages into one
    public static void sendOrders(List<Order> orders) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Schemas.Topics.ORDERS.keySerde().serializer(), Schemas.Topics.ORDERS.valueSerde().serializer());
        for (Order order : orders)
            try {
                ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDERS.name(), order.getId(), order)).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        ordersProducer.close();
    }

    public static void sendOrderValuations(List<OrderValidation> orderValidations) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Schemas.Topics.ORDER_VALIDATIONS.keySerde().serializer(), Schemas.Topics.ORDER_VALIDATIONS.valueSerde().serializer());
        for (OrderValidation ov : orderValidations)
            try {
                ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDER_VALIDATIONS.name(), ov.getOrderId(), ov)).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        ordersProducer.close();
    }

    public static void sendInventory(List<KeyValue<ProductType, Integer>> inventory, Schemas.Topic<ProductType, Integer> topic) {
        KafkaProducer<ProductType, Integer> stockProducer = new KafkaProducer<>(producerConfig(CLUSTER), topic.keySerde().serializer(), Schemas.Topics.WAREHOUSE_INVENTORY.valueSerde().serializer());
        for (KeyValue kv : inventory)
            try {
                stockProducer.send(new ProducerRecord(Schemas.Topics.WAREHOUSE_INVENTORY.name(), kv.key, kv.value)).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        stockProducer.close();
    }
}
