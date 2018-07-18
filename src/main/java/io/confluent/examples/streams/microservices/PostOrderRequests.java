package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.Paths;
import org.apache.kafka.streams.KeyValue;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import java.util.ArrayList;
import java.util.List;

import static io.confluent.examples.streams.avro.microservices.Product.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.Product.UNDERPANTS;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.MIN;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public class PostOrderRequests {

  private static GenericType<OrderBean> newBean() {
    return new GenericType<OrderBean>() {
    };
  }

  public static void main(String [] args) throws Exception {

    final String HOST = "localhost";
    List<Service> services = new ArrayList<>();
    int restPort;
    OrderBean returnedBean;
    Paths path = new Paths("localhost", 5432);

    final ClientConfig clientConfig = new ClientConfig();
    clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 60000)
        .property(ClientProperties.READ_TIMEOUT, 60000);
    Client client = ClientBuilder.newClient(clientConfig);

    // send 2000 orders, one every 1000 milliseconds
    for (int i = 0; i < 2000; i++) {
      OrderBean inputOrder = new OrderBean(id(i), 2L, OrderState.CREATED, Product.JUMPERS, 1, 1d);

      // POST order
      client.target(path.urlPost()).request(APPLICATION_JSON_TYPE).post(Entity.json(inputOrder));

      // GET order, assert that it is Validated
      returnedBean = client.target(path.urlGetValidated(i)).queryParam("timeout", MIN)
          .request(APPLICATION_JSON_TYPE).get(newBean());
      //assertThat(returnedBean).isEqualTo(new OrderBean(
      //    inputOrder.getId(),
      //    inputOrder.getCustomerId(),
      //    OrderState.VALIDATED,
      //    inputOrder.getProduct(),
      //    inputOrder.getQuantity(),
      //    inputOrder.getPrice()
      //));

      Thread.sleep(1000L);
    }
  }

}
