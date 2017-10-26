package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.interactivequeries.MetadataService;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.domain.beans.OrderId;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ManagedAsync;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;
import static io.confluent.examples.streams.microservices.domain.beans.OrderBean.fromBean;
import static io.confluent.examples.streams.microservices.domain.beans.OrderBean.toBean;
import static org.apache.kafka.streams.state.StreamsMetadata.NOT_AVAILABLE;

/**
 * This class provides a REST interface to write and read orders using a CQRS pattern
 * (https://martinfowler.com/bliki/CQRS.html).
 * Two methods are exposed over HTTP:
 * - POST(Order) -> Writes and order. Returns location of Validated or Failed Order.
 * - GET(OrderId, Optional timeout) -> Returns requested order, blocking for timeout if no id present.
 * <p>
 * POST does what you might expect: it adds an Order to the system returning when Kafka sends the appropriate
 * acknowledgement.
 * <p>
 * GET accesses an inbuilt Materialized View, of Orders, which are kept in a State Store
 * inside the service. This CQRS-styled view is updated asynchronously wrt the HTTP POST.
 * <p>
 * Calling GET(id) when the ID is not present will block the caller until either the order is
 * added to the view, or the passed TIMEOUT period elapses. This allows the caller to read-their-own-writes.
 * <p>
 * In addition HTTP POST returns the location of the VALIDATED or FAILED order in the location section
 * of the HTTP POST response. If the Client performs a GET on this returned Location the call block on
 * the server until the FAILED/VALIDATED order is available in the View.
 * <p>
 * The View can also be scaled out linearly simply by adding more instances of the view service, and
 * requests to any of the REST endpoints will be automatically forwarded to the correct instance for
 * the key requested orderId via Kafka's Queryable State feature.
 * <p>
 * Non-blocking IO is used for all operations other than the intialization of state stores on startup or
 * rebalase whcih will block calling Jetty thread.
 */
@Path("orders")
public class OrdersService implements Service {
    private static final String LONG_POLL_TIMEOUT = "10000";
    private static final String ORDERS_STORE_NAME = "orders-store";
    private final String SERVICE_APP_ID = getClass().getSimpleName();
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private Server jettyServer;
    private HostInfo hostInfo;
    private KafkaStreams streams;
    private MetadataService metadataService;
    private KafkaProducer<String, Order> producer;

    //In a real implementation we would need to periodically purge old entries from this map.
    private Map<String, AsyncResponse> outstandingRequests = new ConcurrentHashMap<>();

    public OrdersService(HostInfo hostInfo) {
        this.hostInfo = hostInfo;
    }

    /**
     * Create a table of orders which we can query. When the table is updated
     * we check to see if there is an outstanding HTTP GET request waiting to be
     * fulfilled.
     *
     * @return
     */
    private KStreamBuilder createOrdersMaterializedView() {
        KStreamBuilder builder = new KStreamBuilder();
        builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .groupByKey(ORDERS.keySerde(), ORDERS.valueSerde())
                .reduce((agg, newVal) -> newVal, ORDERS_STORE_NAME)
                .toStream().foreach(this::maybeCompleteLongPollGet);
        return builder;
    }

    private void maybeCompleteLongPollGet(String id, Order order) {
        AsyncResponse callback = outstandingRequests.get(id);
        if (callback != null)
            callback.resume(toBean(order));
    }

    /**
     * Perform a "Long-Poll" styled get. This method will attempt to get the value for the passed key
     * blocking until the key is available or passed timeout is reached. Non-blocking IO is used to
     * implement this, but the API will block the calling thread if no metastore data is available
     * (for example on startup or during a rebalance)
     *
     * @param id            - the key of the value to retrieve
     * @param timeout       - the timeout for the long-poll
     * @param asyncResponse - async response used to trigger the poll early should the appropriate value become available
     */
    @GET
    @ManagedAsync
    @Path("order/{id}")
    public void getWithTimeout(@PathParam("id") final String id,
                               @QueryParam("timeout") @DefaultValue(LONG_POLL_TIMEOUT) Long timeout,
                               @Suspended final AsyncResponse asyncResponse) {
        setTimeout(timeout, asyncResponse);

        HostStoreInfo hostForKey = getKeyLocationOrBlock(id, asyncResponse);

        if (hostForKey == null) //request timed out so return
            return;

        //Retrieve the order locally or reach out to a different instance if the required partition is hosted elsewhere.
        if (thisHost(hostForKey))
            fetchLocal(id, asyncResponse);
        else
            fetchFromOtherHost(hostForKey, "orders/order/" + id, asyncResponse);
    }

    private void fetchLocal(String id, AsyncResponse asyncResponse) {
        System.out.println("running GET on this node");
        try {
            Order order = streams.store(ORDERS_STORE_NAME, QueryableStoreTypes.<String, Order>keyValueStore()).get(id);
            if (order == null) {
                System.out.println("Delaying get as order not present for id " + id);
                outstandingRequests.put(id, asyncResponse);
            } else {
                asyncResponse.resume(toBean(order));
            }
        } catch (InvalidStateStoreException e) {
            //Store not ready so delay
            outstandingRequests.put(id, asyncResponse);
        }
    }

    /**
     * The key could be located in a state store on this node, or on a different node.
     * Check if location metadata is available and if it isn't, which can happen on startup
     * or during a rebalance, block until it is.
     */
    private HostStoreInfo getKeyLocationOrBlock(String id, AsyncResponse asyncResponse) {
        HostStoreInfo locationOfKey;
        while (locationMetadataIsUnavailable(locationOfKey = getHostForOrderId(id))) {
            //The metastore is not available. This can happen on startup/rebalance.
            if (asyncResponse.isDone())
                //The response timed out so return
                return null;
            try {
                //Sleep a bit until metadata becomes available
                Thread.sleep(Math.min(Long.valueOf(LONG_POLL_TIMEOUT), 200));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return locationOfKey;
    }

    private boolean locationMetadataIsUnavailable(HostStoreInfo hostWithKey) {
        return NOT_AVAILABLE.host().equals(hostWithKey.getHost())
                && NOT_AVAILABLE.port() == hostWithKey.getPort();
    }

    private boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }

    private void fetchFromOtherHost(final HostStoreInfo host, final String path, AsyncResponse asyncResponse) {
        System.out.println("Chaining GET to a different instance: " + host);
        try {
            OrderBean bean = client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
                    .request(MediaType.APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });
            asyncResponse.resume(bean);
        } catch (Exception swallowed) {
        }
    }

    /**
     * Persist an Order to Kafka. Returns once the order is successfully written to R nodes where
     * R is the replication factor configured in Kafka.
     *
     * @param order   the order to add
     * @param timeout the max time to wait for the response from Kafka before timing out the POST
     */
    @POST
    @ManagedAsync
    @Path("/post")
    @Consumes(MediaType.APPLICATION_JSON)
    public void submitOrder(final OrderBean order,
                            @QueryParam("timeout") @DefaultValue(LONG_POLL_TIMEOUT) final Long timeout,
                            @Suspended final AsyncResponse response) {
        setTimeout(timeout, response);

        Order bean = fromBean(order);
        producer.send(new ProducerRecord<>(ORDERS.name(), bean.getId(), bean),
                callback(response, bean.getId()));
    }

    @Override
    public void start(String bootstrapServers) {
        producer = startProducer(bootstrapServers, ORDER_VALIDATIONS);
        streams = startKStreams(bootstrapServers);
        jettyServer = startJetty(hostInfo.port(), this);
        System.out.println("Started Service " + getClass().getSimpleName());
    }

    private KafkaStreams startKStreams(String bootstrapServers) {
        KafkaStreams streams = new KafkaStreams(createOrdersMaterializedView(), config(bootstrapServers));
        metadataService = new MetadataService(streams);
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        return streams;
    }

    private Properties config(String bootstrapServers) {
        Properties props = baseStreamsConfig(bootstrapServers, "/tmp/kafka-streams", SERVICE_APP_ID);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo.host() + ":" + hostInfo.port());
        return props;
    }

    @Override
    public void stop() {
        if (streams != null) streams.close();
        if (producer != null) {
            producer.close();
        }
        if (jettyServer != null) {
            try {
                jettyServer.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private HostStoreInfo getHostForOrderId(String orderId) {
        return metadataService.streamsMetadataForStoreAndKey(ORDERS_STORE_NAME, orderId, Serdes.String().serializer());
    }

    private Callback callback(final AsyncResponse response, final String orderId) {
        return (recordMetadata, e) -> {
            if (e != null)
                response.resume(e);
            else
                try {
                    //Return the location of the newly created resource
                    //This corresponds to the Validated or Failed order (orderVerson+1)
                    String newId = OrderId.next(orderId);
                    Response uri = Response.created(new URI("/orders/order/" + newId))
                            .entity(newId)
                            .build();
                    response.resume(uri);
                } catch (URISyntaxException e2) {
                    e2.printStackTrace();
                }
        };
    }

    public static void main(String[] args) throws Exception {

        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";
        final String restHostname = args.length > 3 ? args[3] : "localhost";
        final String restPort = args.length > 4 ? args[4] : Integer.toString(randomFreeLocalPort());

        Schemas.configureSerdesWithSchemaRegistryUrl(schemaRegistryUrl);
        OrdersService service = new OrdersService(new HostInfo(restHostname, Integer.valueOf(restPort)));
        service.start(bootstrapServers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
    }
}