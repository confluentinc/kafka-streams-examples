package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.beans.OrderId;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

import static io.confluent.examples.streams.avro.microservices.Order.newBuilder;
import static io.confluent.examples.streams.avro.microservices.OrderType.VALIDATED;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;


/**
 * A simple service which listens to to validation results from each of the Validation
 * services and aggregates them by order Id, triggering a pass or fail based on whether
 * all rules pass or not.
 */
public class RuleAggregatorService implements Service {
    private static final String ORDERS_SERVICE_APP_ID = "orders-service";
    private static final long MIN = 60 * 1000L;
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers) {
        streams = aggregateOrderValidations(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        System.out.println("Started Service " + getClass().getSimpleName());
    }

    private KafkaStreams aggregateOrderValidations(String bootstrapServers, String stateDir) {
        final int numberOfRules = 3; //TODO put into a KTable to make dynamically configurable

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, OrderValidation> validations = builder.stream(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());
        KStream<String, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //If all rules pass then validate the order
        validations.groupByKey(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde())
                .aggregate(
                        () -> 0L,
                        (id, result, total) -> PASS.equals(result.getValidationResult()) ? total + 1 : total,
                        TimeWindows.of(5 * MIN).advanceBy(1 * MIN), //5 min window that slides forward every minute.
                        Serdes.Long()
                )
                //get rid of window
                .toStream((windowedKey, total) -> windowedKey.key())
                //only include results were all rules passed validation
                .filter((k, total) -> total >= numberOfRules)
                //Join back to orders
                .join(orders, (id, order) ->
                                //Set the order to Validated and bump the version on it's ID
                                newBuilder(order)
                                        .setState(VALIDATED)
                                        .setId(OrderId.next(order.getId())).build()
                        , JoinWindows.of(3000 * 1000L), ORDERS.keySerde(), Serdes.Long(), ORDERS.valueSerde())
                //Rekey (as we bumped the version on the OrderID)
                .selectKey((k, v) -> v.getId())
                //Push the validated order into the orders topic
                .to(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());

        //If any rule fails then fail the order
        validations.filter((id, rule) -> FAIL.equals(rule.getValidationResult()))
                .join(orders, (id, order) ->
                                //Set the order to Failed and bump the version on it's ID
                                newBuilder(order)
                                        .setId(OrderId.next(order.getId()))
                                        .setState(OrderType.FAILED).build(),
                        JoinWindows.of(3000 * 1000L), ORDERS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDERS.valueSerde())
                //there could be multiple failed rules for each order so collapse to a single order
                .groupByKey(ORDERS.keySerde(), ORDERS.valueSerde())
                .reduce((order, v1) -> order)
                //Rekey (as we bumped the version on the OrderID above)
                .toStream().selectKey((k, v) -> v.getId())
                //Push the validated order into the orders topic
                .to(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());

        return new KafkaStreams(builder, baseStreamsConfig(bootstrapServers, stateDir, ORDERS_SERVICE_APP_ID));
    }

    @Override
    public void stop() {
        if (streams != null) streams.close();
    }

    public static void main(String[] args) throws Exception {

        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";

        Schemas.configureSerdesWithSchemaRegistryUrl(schemaRegistryUrl);
        RuleAggregatorService service = new RuleAggregatorService();
        service.start(bootstrapServers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
    }
}