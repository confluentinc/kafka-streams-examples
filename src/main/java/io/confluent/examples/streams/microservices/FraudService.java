package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValue;
import io.confluent.examples.streams.microservices.domain.Schemas;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.FRAUD_CHECK;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.initSchemaRegistryAndGetBootstrapServers;


/**
 * This service searches for potentially fraudulent transactions by calculating the total value of orders for a
 * customer within a time period, then checks to see if this is over a configured limit.
 * <p>
 * i.e. if(SUM(order.value, 5Mins) > $5000) GroupBy customer -> Fail(orderId) else Pass(orderId)
 */
public class FraudService implements Service {
    private static final String FRAUD_SERVICE_APP_ID = "fraud-service";
    private static final int FRAUD_LIMIT = 2000;
    private static final long MIN = 60 * 1000L;
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers) {
        streams = processOrders(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        System.out.println("Started Service " + getClass().getSimpleName());
    }


    private KafkaStreams processOrders(final String bootstrapServers,
                                       final String stateDir) {

        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //Create an aggregate of the total value by customer and hold it with the order.
        //We use a trick to make this work: we disable caching so we get a complete changelog stream.
        KTable<Windowed<Long>, OrderValue> aggregate = orders
                .groupBy((id, order) -> order.getCustomerId(), Serdes.Long(), ORDERS.valueSerde())
                .aggregate(OrderValue::new,
                        (custId, order, orderValue) -> new OrderValue(
                                order, orderValue.getValue() + order.getQuantity() * order.getPrice()),
                        TimeWindows.of(5 * MIN),
                        //TODO I believe this will fail when the window crosses the epoch boundary, so I want something
                        //TODO like TimeWindows.of(5 * MIN).advanceBy(1 * MIN) but this doesn't work. Not sure why?
                        Schemas.ORDER_VALUE_SERDE);

        //Ditch the windowing and rekey
        KStream<String, OrderValue> ordersWithTotals = aggregate
                .toStream((windowedKey, orderValue) -> windowedKey.key())
                .selectKey((id, orderValue) -> orderValue.getOrder().getId());

        //Now branch the stream into two, for pass and fail, based on whether the windowed total is over Fraud Limit
        KStream<String, OrderValue>[] forks = ordersWithTotals.branch(
                (id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT,
                (id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT);

        forks[0].mapValues((orderValue) -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, FAIL))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        forks[1].mapValues((pair) -> new OrderValidation(pair.getOrder().getId(), FRAUD_CHECK, PASS))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        //disable caching to ensure a complete aggregate changelog. This is a little trick we need to apply
        //as caching in Kafka Streams will conflate subsequent updates for the same key. Disabling caching ensures
        //we get a complete "changelog" from the aggregate(...) step above (i.e. every input event will have a
        //corresponding output event.
        Properties props = baseStreamsConfig(bootstrapServers, stateDir, FRAUD_SERVICE_APP_ID);
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        return new KafkaStreams(builder, props);
    }

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = initSchemaRegistryAndGetBootstrapServers(args);
        FraudService service = new FraudService();
        service.start(bootstrapServers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
    }

    @Override
    public void stop() {
        if (streams != null) streams.close();
    }
}