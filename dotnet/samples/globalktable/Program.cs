using com.dotnet.samples.avro;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Threading;

namespace GlobalKTable
{
    /// Demonstrates how to perform joins between  KStreams and GlobalKTables, i.e. joins that
    /// don't require re-partitioning of the input streams.
    /// <para>
    /// In this example, we join a stream of orders that reads from a topic named
    /// "order" with a customers table that reads from a topic named "customer", and a products
    /// table that reads from a topic "product". The join produces an EnrichedOrder object.
    /// </para>
    /// <para>
    /// HOW TO RUN THIS EXAMPLE
    /// </para>
    /// <para>
    /// 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
    /// </para>
    /// <para>
    /// 2) Create the input/intermediate/output topics used by this example.
    /// </para>
    /// <code>
    /// $ bin/kafka-topics --create --topic order \
    ///                    --zookeeper localhost:2181 --partitions 4 --replication-factor 1
    /// $ bin/kafka-topics --create --topic customer \
    ///                    --zookeeper localhost:2181 --partitions 3 --replication-factor 1
    /// $ bin/kafka-topics --create --topic product \
    ///                    --zookeeper localhost:2181 --partitions 2 --replication-factor 1
    /// $ bin/kafka-topics --create --topic enriched-order \
    ///                    --zookeeper localhost:2181 --partitions 4 --replication-factor 1
    /// </code>
    /// Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
    /// `bin/kafka-topics.sh ...`.
    /// <para>
    /// 3) Start this example application either in your IDE or on the command line.
    /// </para>
    /// <para>
    /// 4) Write some input data to the source topics. The
    /// already running example application (step 3) will automatically process this input data and write
    /// the results to the output topic.
    /// </para>
    /// <para>
    /// 5) Once you're done with your experiments, you can stop this example via Ctrl-C. If needed,
    /// also stop the Confluent Schema Registry (Ctrl-C), then stop the Kafka broker (Ctrl-C), and
    /// only then stop the ZooKeeper instance (Ctrl-C).
    /// </para>
    class Program
    {
        static readonly string ORDER_TOPIC = "order";
        static readonly string CUSTOMER_TOPIC = "customer";
        static readonly string PRODUCT_TOPIC = "product";
        static readonly string CUSTOMER_STORE = "customer-store";
        static readonly string PRODUCT_STORE = "product-store";
        static readonly string ENRICHED_ORDER_TOPIC = "enriched-order";

        static void Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            // IN PROGRESS
            string bootstrapServers = args.Length > 0 ? args[0] : "localhost:9092";
            string schemaRegistryUrl = args.Length > 1 ? args[1] : "http://localhost:8081";

            var config = new StreamConfig<Int64SerDes, StringSerDes>();
            config.ApplicationId = "global-tables-example";
            config.ClientId = "global-tables-example-client";
            // Where to find Kafka broker(s).
            config.BootstrapServers = bootstrapServers;
            // Set to earliest so we don't miss any data that arrived in the topics before the process
            // started
            config.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            config.SchemaRegistryUrl = schemaRegistryUrl;
            config.AutoRegisterSchemas = true;

            var t = GetTopology();

            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }

        static Topology GetTopology()
        {
            StreamBuilder builder = new StreamBuilder();

            var orderStream = builder.Stream<long, Order, Int64SerDes, SchemaAvroSerDes<Order>>(ORDER_TOPIC);

            var customers = builder.GlobalTable<long, Customer, Int64SerDes, SchemaAvroSerDes<Customer>>(
                CUSTOMER_TOPIC, InMemory<long, Customer>.As(CUSTOMER_STORE));

            var products = builder.GlobalTable<long, Product, Int64SerDes, SchemaAvroSerDes<Product>>(
                PRODUCT_TOPIC, InMemory<long, Product>.As(PRODUCT_STORE));

            var customerOrderStream =
                orderStream.Join(customers,
                                    (orderId, order) => order.customerId,
                                    (order, customer) => new CustomerOrder(customer, order));

            var enrichedOrderStream =
                customerOrderStream.Join(products,
                                            (orderId, customerOrder) => customerOrder.Order.productId,
                                            (customerOrder, product) =>
                                                 new EnrichedOrder(product,
                                                                    customerOrder.Customer,
                                                                    customerOrder.Order));

            enrichedOrderStream.To<Int64SerDes, SchemaAvroSerDes<EnrichedOrder>>(ENRICHED_ORDER_TOPIC);

            return builder.Build();
        }
    }
}