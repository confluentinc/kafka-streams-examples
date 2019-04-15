package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.examples.streams.microservices.util.Paths;
import org.apache.kafka.streams.KeyValue;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import java.util.ArrayList;
import java.util.List;

import static io.confluent.examples.streams.avro.microservices.Product.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.Product.UNDERPANTS;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.MIN;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class EndToEndTest extends MicroserviceTestUtils {

  private static final Logger log = LoggerFactory.getLogger(EndToEndTest.class);
  private static final String HOST = "localhost";
  private final List<Service> services = new ArrayList<>();
  private OrderBean returnedBean;
  private long startTime;
  private Paths path;
  private Client client;

  @Test
  public void shouldCreateNewOrderAndGetBackValidatedOrder() {
    final OrderBean inputOrder = new OrderBean(id(1L), 2L, OrderState.CREATED, Product.JUMPERS, 1, 1d);
    client = getClient();

    //Add inventory required by the inventory service with enough items in stock to pass validation
    final List<KeyValue<Product, Integer>> inventory = asList(
      new KeyValue<>(UNDERPANTS, 75),
      new KeyValue<>(JUMPERS, 10)
    );
    sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

    //When we POST order and immediately GET on the returned location
    postWithRetries(client.target(path.urlPost()).request(APPLICATION_JSON_TYPE), Entity.json(inputOrder), 5);
    final Invocation.Builder builder = client
      .target(path.urlGetValidated(1))
      .queryParam("timeout", MIN)
      .request(APPLICATION_JSON_TYPE);
    returnedBean = getWithRetries(builder, newBean(),5);

    //Then
    assertThat(returnedBean.getState()).isEqualTo(OrderState.VALIDATED);
  }

  @Test
  public void shouldProcessManyValidOrdersEndToEnd() {
    client = getClient();

    //Add inventory required by the inventory service
    final List<KeyValue<Product, Integer>> inventory = asList(
      new KeyValue<>(UNDERPANTS, 75),
      new KeyValue<>(JUMPERS, 10)
    );
    sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

    //Send ten orders in succession
    for (int i = 0; i < 10; i++) {
      final OrderBean inputOrder = new OrderBean(id(i), 2L, OrderState.CREATED, Product.JUMPERS, 1, 1d);

      startTimer();

      //POST & GET order
      postWithRetries(client.target(path.urlPost()).request(APPLICATION_JSON_TYPE), Entity.json(inputOrder), 5);
      final Invocation.Builder builder = client
        .target(path.urlGetValidated(i))
        .queryParam("timeout", MIN)
        .request(APPLICATION_JSON_TYPE);
      returnedBean = getWithRetries(builder, newBean(),5);

      endTimer();

      assertThat(returnedBean).isEqualTo(new OrderBean(
        inputOrder.getId(),
        inputOrder.getCustomerId(),
        OrderState.VALIDATED,
        inputOrder.getProduct(),
        inputOrder.getQuantity(),
        inputOrder.getPrice()
      ));
    }
  }

  @Test
  public void shouldProcessManyInvalidOrdersEndToEnd() {
    client = getClient();

    //Add inventory required by the inventory service
    final List<KeyValue<Product, Integer>> inventory = asList(
      new KeyValue<>(UNDERPANTS, 75000),
      new KeyValue<>(JUMPERS, 0) //***nothing in stock***
    );
    sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

    //Send ten orders one after the other
    for (int i = 0; i < 10; i++) {
      final OrderBean inputOrder = new OrderBean(id(i), 2L, OrderState.CREATED, Product.JUMPERS, 1, 1d);

      startTimer();

      //POST & GET order
      postWithRetries(client.target(path.urlPost()).request(APPLICATION_JSON_TYPE), Entity.json(inputOrder), 5);
      final Invocation.Builder builder = client
        .target(path.urlGetValidated(i))
        .queryParam("timeout", MIN)
        .request(APPLICATION_JSON_TYPE);
      returnedBean = getWithRetries(builder, newBean(), 5);

      endTimer();

      assertThat(returnedBean).isEqualTo(new OrderBean(
        inputOrder.getId(),
        inputOrder.getCustomerId(),
        OrderState.FAILED,
        inputOrder.getProduct(),
        inputOrder.getQuantity(),
        inputOrder.getPrice()
      ));
    }
  }

  private Client getClient() {
    final ClientConfig clientConfig = new ClientConfig();
    clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 60000)
      .property(ClientProperties.READ_TIMEOUT, 60000);
    return ClientBuilder.newClient(clientConfig);
  }

  private void startTimer() {
    startTime = System.currentTimeMillis();
  }

  private void endTimer() {
    log.info("Took: " + (System.currentTimeMillis() - startTime));
  }

  @Before
  public void startEverythingElse() throws Exception {
    if (!CLUSTER.isRunning()) {
      CLUSTER.start();
    }

    Topics.ALL.keySet().forEach(CLUSTER::createTopic);
    Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());

    services.add(new FraudService());
    services.add(new InventoryService());
    services.add(new OrderDetailsService());
    services.add(new ValidationsAggregatorService());

    tailAllTopicsToConsole(CLUSTER.bootstrapServers());
    services.forEach(s -> s.start(CLUSTER.bootstrapServers()));

    final OrdersService ordersService = new OrdersService(HOST, 0);
    ordersService.start(CLUSTER.bootstrapServers());
    path = new Paths("localhost", ordersService.port());
    services.add(ordersService);
  }

  @After
  public void tearDown() {
    services.forEach(Service::stop);
    stopTailers();
    CLUSTER.stop();
    if (client != null) {
      client.close();
    }
  }

  private GenericType<OrderBean> newBean() {
    return new GenericType<OrderBean>() {
    };
  }
}
