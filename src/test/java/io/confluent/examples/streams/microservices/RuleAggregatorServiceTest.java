package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.avro.microservices.OrderValidationType;
import io.confluent.examples.streams.microservices.RuleAggregatorService;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import org.apache.kafka.streams.KeyValue;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static io.confluent.examples.streams.avro.microservices.OrderType.*;
import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class RuleAggregatorServiceTest extends MicroserviceTestUtils {
    private List<Order> orders;
    private List<OrderValidation> ruleResults;
    private RuleAggregatorService ordersService;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Topics.ORDERS.name());
        CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @Test
    public void shouldAggregateRuleSuccesses() throws Exception {

        //Given
        ordersService = new RuleAggregatorService();

        orders = asList(
                new Order(id(0L), 0L, CREATED, UNDERPANTS, 3, 5.00d),
                new Order(id(1L), 0L, CREATED, JUMPERS, 1, 75.00d)
        );
        sendOrders(orders);

        ruleResults = asList(
                new OrderValidation(id(0L), OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS),
                new OrderValidation(id(0L), OrderValidationType.ORDER_DETAILS_CHECK, OrderValidationResult.PASS),
                new OrderValidation(id(0L), OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS),
                new OrderValidation(id(1L), OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS),
                new OrderValidation(id(1L), OrderValidationType.ORDER_DETAILS_CHECK, OrderValidationResult.FAIL),
                new OrderValidation(id(1L), OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS)
        );
        sendOrderValuations(ruleResults);

        //When
        ordersService.start(CLUSTER.bootstrapServers());

        //Then
        List<KeyValue<String, Order>> finalOrders = MicroserviceTestUtils.readKeyValues(Topics.ORDERS, 4, CLUSTER.bootstrapServers());
        assertThat(finalOrders.size()).isEqualTo(4);

        //And the first order should have been validated but the second should have failed
        assertThat(finalOrders.stream().map(kv -> kv.value).collect(Collectors.toList())).contains(
                new Order(id(0L, 1), 0L, VALIDATED, UNDERPANTS, 3, 5.00d),
                new Order(id(1L, 1), 0L, FAILED, JUMPERS, 1, 75.00d)
        );
        //And ensure the key in the topic has also been mapped
        assertThat(finalOrders.stream().map(kv -> kv.key).collect(Collectors.toList())).contains(
                id(0L, 1),
                id(1L, 1)
        );
    }


    @After
    public void tearDown() {
        ordersService.stop();
    }
}
