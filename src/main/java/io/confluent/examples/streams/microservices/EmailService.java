package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.CUSTOMERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.PAYMENTS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.MIN;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.addShutdownHookAndBlock;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.parseArgsAndConfigure;

import io.confluent.examples.streams.avro.microservices.Customer;
import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.Payment;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A very simple service which sends emails. Order and Payment streams are joined
 * using a window. The result is then joined to a lookup table of Customers.
 * Finally an email is sent for each resulting tuple.
 */
public class EmailService implements Service {

  private static final Logger log = LoggerFactory.getLogger(EmailService.class);
  private static final String APP_ID = "email-service";
  public static final Joined<String, Order, Payment> serdes = Joined
      .with(ORDERS.keySerde(), ORDERS.valueSerde(), PAYMENTS.valueSerde());

  private KafkaStreams streams;
  private Emailer emailer;
  private Joined<String, Order, Payment> serdes4 = Joined
      .with(ORDERS.keySerde(), ORDERS.valueSerde(), PAYMENTS.valueSerde());

  public EmailService(final Emailer emailer) {
    this.emailer = emailer;
  }

  @Override
  public void start(final String bootstrapServers) {
    streams = processStreams(bootstrapServers, "/tmp/kafka-streams");
    streams.cleanUp(); //don't do this in prod as it clears your state stores
    final CountDownLatch startLatch = new CountDownLatch(1);
    streams.setStateListener((newState, oldState) -> {
      if (newState == State.RUNNING && oldState == State.REBALANCING) {
        startLatch.countDown();
      }

    });
    streams.start();
    try {
      if (!startLatch.await(60, TimeUnit.SECONDS)) {
        throw new RuntimeException("Streams never finished rebalancing on startup");
      }
    } catch (final InterruptedException e) {
       Thread.currentThread().interrupt();
    }
    log.info("Started Service " + APP_ID);
  }

  private KafkaStreams processStreams(final String bootstrapServers, final String stateDir) {

    final KStreamBuilder builder = new KStreamBuilder();

    //Create the streams/tables for the join
    final KStream<String, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());
    KStream<String, Payment> payments = builder.stream(PAYMENTS.keySerde(), PAYMENTS.valueSerde(), PAYMENTS.name());
    final GlobalKTable<Long, Customer> customers =
      builder.globalTable(CUSTOMERS.keySerde(), CUSTOMERS.valueSerde(), CUSTOMERS.name());

    //Rekey payments to be by OrderId for the windowed join
    payments = payments.selectKey((s, payment) -> payment.getOrderId());

    //Join the two streams and the table then send an email for each
    orders.join(payments, EmailTuple::new,
        //Join Orders and Payments streams
        JoinWindows.of(1 * MIN), serdes)
        //Next join to the GKTable of Customers
        .join(customers,
            (key1, tuple) -> tuple.order.getCustomerId(),
            // note how, because we use a GKtable, we can join on any attribute of the Customer.
            (tuple, customer) -> tuple.setCustomer(customer))
        //Now for each tuple send an email.
        .peek((key, emailTuple)
            -> emailer.sendEmail(emailTuple)
        );

    return new KafkaStreams(builder, baseStreamsConfig(bootstrapServers, stateDir, APP_ID));
  }

  public static void main(final String[] args) throws Exception {
    final EmailService service = new EmailService(new LoggingEmailer());
    service.start(parseArgsAndConfigure(args));
    addShutdownHookAndBlock(service);
  }

  private static class LoggingEmailer implements Emailer {

    @Override
    public void sendEmail(final EmailTuple details) {
      //In a real implementation we would do something a little more useful
      log.warn("Sending an email to: \nCustomer:%s\nOrder:%s\nPayment%s", details.customer,
          details.order, details.payment);
    }
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  interface Emailer {
    void sendEmail(EmailTuple details);
  }

  public class EmailTuple {

    public Order order;
    public Payment payment;
    public Customer customer;

    public EmailTuple(final Order order, final Payment payment) {
      this.order = order;
      this.payment = payment;
    }

    EmailTuple setCustomer(final Customer customer) {
      this.customer = customer;
      return this;
    }

    @Override
    public String toString() {
      return "EmailTuple{" +
          "order=" + order +
          ", payment=" + payment +
          ", customer=" + customer +
          '}';
    }
  }
}
