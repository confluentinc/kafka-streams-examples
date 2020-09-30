using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;
using System.Collections.Generic;
using System.Linq;

namespace GlobalKTableTests
{
    /// <summary>
    /// End-to-end integration test based on GlobalKTable samples.
    /// </summary>
    public class GlobalKTableTest
    {
        static readonly string customerTopicName = "CUSTOMER_TOPIC";
        static readonly string orderTopicName = "order";
        static readonly string joinOrderTopicName = "output_order";

        [Test]
        public void ShouldGlobalKTable()
        {
            List<KeyValuePair<string, string>> inputValues = new List<KeyValuePair<string, string>>{
                KeyValuePair.Create("key1", "1"),
                KeyValuePair.Create("key1", "2"),
                KeyValuePair.Create("key1", "3"),
                KeyValuePair.Create("key2", "10")
            };

            List<KeyValuePair<string, string>> expected = new List<KeyValuePair<string, string>>
            {
                KeyValuePair.Create("key1", "3"),
                KeyValuePair.Create("key2", "10")
            };

            // Step 1: Configure and start the processor topology.
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "globalktable-test";
            config.BootstrapServers = "dummy config";

            var builder = new StreamBuilder();

            var table = builder.GlobalTable(
                customerTopicName,
                InMemory<string, string>.As("global-store"));

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                // Step 2: Setup input and output topics.
                var inputTopic = driver.CreateInputTopic<string, string>(customerTopicName);

                // Step 3: Write the input.
                inputTopic.PipeInputs(inputValues);

                // Step 4: Validate the output.
                var store = driver.GetKeyValueStore<string, string>("global-store");
                long itemsCount = store.ApproximateNumEntries();
                Assert.AreEqual(2, itemsCount);
                var items = store.All().ToList();
                Assert.AreEqual(expected, items);
            }
        }

        [Test]
        public void ShouldGlobalKTableJoinStream()
        {
            // CUSTOMER DATA
            // KEY: CUSTOMER_ID | VALUE : CUSTOMER_NAME

            // ORDER DATA
            // KEY: ORDER_ID | VALUE : CUSTOMER_ID|PRODUCT_NAME

            List<KeyValuePair<string, string>> inputCustomer = new List<KeyValuePair<string, string>>{
                KeyValuePair.Create("cus1", "Customer1"),
                KeyValuePair.Create("cus2", "Customer2")
            };

            List<KeyValuePair<string, string>> inputOrder = new List<KeyValuePair<string, string>>{
                KeyValuePair.Create("order1", "cus1|iPhone"),
                KeyValuePair.Create("order2", "cus2|Gun"),
                KeyValuePair.Create("order3", "cus1|Pen")
            };

            List<KeyValuePair<string, string>> expected = new List<KeyValuePair<string, string>>
            {
                KeyValuePair.Create("order1", "Product:iPhone-Customer:Customer1"),
                KeyValuePair.Create("order2", "Product:Gun-Customer:Customer2"),
                KeyValuePair.Create("order3", "Product:Pen-Customer:Customer1")
            };

            // Step 1: Configure and start the processor topology.
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "globalktable-test";
            config.BootstrapServers = "dummy config";

            var builder = new StreamBuilder();

            var orderStream = builder.Stream<string, string>(orderTopicName);

            var customerOrderStream = orderStream.SelectKey((k, v) => v.Split("|")[0]);

            var customerTable = builder.GlobalTable(customerTopicName,
                                        InMemory<string, string>.As("global-store"));

            var joinOrderStream = orderStream.Join(customerTable,
                                (k, v) => v.Split("|")[0],
                                (order, customer) => $"Product:{order.Split("|")[1]}-Customer:{customer}");

            joinOrderStream.To(joinOrderTopicName);


            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                // Step 2: Setup input and output topics.
                var inputTopicCustomer = driver.CreateInputTopic<string, string>(customerTopicName);
                var inputOrderTopic = driver.CreateInputTopic<string, string>(orderTopicName);
                var outputTopic = driver.CreateOuputTopic<string, string>(joinOrderTopicName);

                // Step 3: Write the input.
                inputTopicCustomer.PipeInputs(inputCustomer);
                inputOrderTopic.PipeInputs(inputOrder);

                // Step 4: Validate the output.
                var items = outputTopic
                    .ReadKeyValueList()
                    .Select(c => KeyValuePair.Create(c.Message.Key, c.Message.Value))
                    .ToList();

                Assert.AreEqual(expected, items);
            }
        }
    }
}