package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.Payment;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.Paths;
import io.confluent.examples.streams.utils.MonitoringInterceptorUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public class PostOrdersAndPayments {

    private static GenericType<OrderBean> newBean() {
        return new GenericType<OrderBean>() {
        };
    }

    private static void sendPayment(final String id,
                                    final Payment payment,
                                    final String bootstrapServers,
                                    final String schemaRegistryUrl) {

        //System.out.printf("-----> id: %s, payment: %s%n", id, payment);

        final SpecificAvroSerializer<Payment> paymentSerializer = new SpecificAvroSerializer<>();
        final boolean isKeySerde = false;
        paymentSerializer.configure(
            Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
            isKeySerde);

        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "payment-generator");
        MonitoringInterceptorUtils.maybeConfigureInterceptorsProducer(producerConfig);

        final KafkaProducer<String, Payment> paymentProducer = new KafkaProducer<>(producerConfig, new StringSerializer(), paymentSerializer);

        final ProducerRecord<String, Payment> record = new ProducerRecord<>("payments", id, payment);
        paymentProducer.send(record);
        paymentProducer.close();
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws Exception {

        final int NUM_CUSTOMERS = 6;
        final List<Product> productTypeList = Arrays.asList(Product.JUMPERS, Product.UNDERPANTS, Product.STOCKINGS);
        final Random randomGenerator = new Random();

        final int restPort = args.length > 0 ? Integer.parseInt(args[0]) : 5432;
        System.out.printf("restPort: %d%n", restPort);
        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";

        OrderBean returnedOrder;
        final Paths path = new Paths("localhost", restPort == 0 ? 5432 : restPort);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonFeature.class);
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 60000)
                    .property(ClientProperties.READ_TIMEOUT, 60000);
        final Client client = ClientBuilder.newClient(clientConfig);

        // send one order every 1 second
        int i = 1;
        while (true) {

            final int randomCustomerId = randomGenerator.nextInt(NUM_CUSTOMERS);
            final Product randomProduct = productTypeList.get(randomGenerator.nextInt(productTypeList.size()));

            final OrderBean inputOrder = new OrderBean(id(i), randomCustomerId, OrderState.CREATED, randomProduct, 1, 1d);

            // POST order to OrdersService
            System.out.printf("Posting order to: %s   .... ", path.urlPost());
            final Response response = client.target(path.urlPost())
                                            .request(APPLICATION_JSON_TYPE)
                                            .post(Entity.json(inputOrder));
            System.out.printf("Response: %s %n", response.getStatus());

            // GET the bean back explicitly
            System.out.printf("Getting order from: %s   .... ", path.urlGet(i));
            returnedOrder = client.target(path.urlGet(i))
                                  .queryParam("timeout", Duration.ofMinutes(1).toMillis() / 2)
                                  .request(APPLICATION_JSON_TYPE)
                                  .get(newBean());

            if (!inputOrder.equals(returnedOrder)) {
                System.out.printf("Posted order %d does not equal returned order: %s%n", i, returnedOrder.toString());
            } else {
                System.out.printf("Posted order %d equals returned order: %s%n", i, returnedOrder.toString());
            }

            // Send payment
            final Payment payment = new Payment("Payment:1234", id(i), "CZK", 1000.00d);
            sendPayment(payment.getId(), payment, bootstrapServers, schemaRegistryUrl);

            Thread.sleep(5000L);
            i++;
        }
    }

}
