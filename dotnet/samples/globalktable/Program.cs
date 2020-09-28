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