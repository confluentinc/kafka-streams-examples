package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.ServerErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.net.URI;

import static io.confluent.examples.streams.avro.microservices.Order.newBuilder;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.randomFreeLocalPort;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.next;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.fail;

public class OrdersServiceTest extends MicroserviceTestUtils {

    private int port;
    private OrdersService rest;
    private OrdersService rest2;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Schemas.Topics.ORDERS.name());
        System.out.println("running with schema registry: " + CLUSTER.schemaRegistryUrl());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @Before
    public void start() throws Exception {
        port = randomFreeLocalPort();
    }

    @After
    public void shutdown() throws Exception {
        if (rest != null)
            rest.stop();
        if (rest2 != null)
            rest2.stop();
    }

    @Test
    public void shouldPostOrderAndGetItBack() throws Exception {
        OrderBean bean = new OrderBean(id(1L), 2L, OrderType.CREATED, ProductType.JUMPERS, 10, 100d);

        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";

        //Given a rest service
        rest = new OrdersService(
                new HostInfo("localhost", port)
        );
        rest.start(CLUSTER.bootstrapServers());

        //When we POST an order
        Response response = client.target(baseUrl + "/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(bean));

        //Then
        assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

        //When GET the bean back
        OrderBean returnedBean = client.target(baseUrl + "/order/" + bean.getId())
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        //Then it should be the bean we PUT
        assertThat(returnedBean).isEqualTo(bean);
    }


    @Test
    public void shouldPostLinkToNextVersionOfRecordInResponse() throws Exception {
        Order orderV1 = new Order(id(1L, 0), 2L, OrderType.CREATED, ProductType.JUMPERS, 10, 100d);
        OrderBean beanV1 = OrderBean.toBean(orderV1);

        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";

        //Given a rest service
        rest = new OrdersService(
                new HostInfo("localhost", port)
        );
        rest.start(CLUSTER.bootstrapServers());

        //When we post an order
        Response response = client.target(baseUrl + "/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(beanV1));

        //Simulate the order being validated
        MicroserviceTestUtils.sendOrders(asList(
                newBuilder(orderV1)
                        .setId(next(orderV1.getId()))
                        .setState(OrderType.VALIDATED)
                        .build()));

        //The URI returned by the POST should point to the 'post validation' order
        URI location = response.getLocation();

        //When we GAT the order from the returned location
        OrderBean returnedBean = client.target(location)
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        //Then Version should have been incremented
        assertThat(returnedBean.getId()).isEqualTo(id(1L, 1));
        //Then status should be Validated
        assertThat(returnedBean.getState()).isEqualTo(OrderType.VALIDATED);
    }

    @Test
    public void shouldTimeoutGetIfNoResponseIsFound() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";


        //Start the rest interface
        rest = new OrdersService(
                new HostInfo("localhost", port)
        );
        rest.start(CLUSTER.bootstrapServers());

        //Then GET order should timeout
        try {
            client.target(baseUrl + "/order/" + id(1))
                    .queryParam("timeout", 100) //Lower the request timeout
                    .request(APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });
            fail("Request should have failed as materialized view has not been updated");
        } catch (ServerErrorException e) {
            assertThat(e.getMessage()).isEqualTo("HTTP 504 Gateway Timeout");
        }
    }

    @Test
    public void shouldGetOrderByIdWhenOnDifferentHost() throws Exception {
        OrderBean order = new OrderBean(id(1L), 2L, OrderType.VALIDATED, ProductType.JUMPERS, 10, 100d);
        int port1 = randomFreeLocalPort();
        int port2 = randomFreeLocalPort();
        final Client client = ClientBuilder.newClient();

        //Given two rest servers on different ports
        rest = new OrdersService(new HostInfo("localhost", port1));
        rest2 = new OrdersService(new HostInfo("localhost", port2));
        rest.start(CLUSTER.bootstrapServers());
        rest2.start(CLUSTER.bootstrapServers());

        //And one order
        client.target("http://localhost:" + port1 + "/orders/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(order));

        //When GET to rest1
        OrderBean returnedOrder = client.target("http://localhost:" + port1 + "/orders/order/" + order.getId())
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        //Then we should get the order back
        assertThat(returnedOrder).isEqualTo(order);

        //When GET to rest2
        returnedOrder = client.target("http://localhost:" + port2 + "/orders/order/" + order.getId())
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        //Then we should get the order back also
        assertThat(returnedOrder).isEqualTo(order);
    }
}
