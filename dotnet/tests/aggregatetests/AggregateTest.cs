using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;
using System.Collections.Generic;
using TestUtils;

namespace AggregateTests
{
    /// <summary>
    /// How to aggregate messages via `groupBy()` and `aggregate()`.
    /// </summary>
    public class AggregateTest
    {
        private static readonly string inputTopicName = "inputTopic";
        private static readonly string outputTopicName = "outputTopic";

        [Test]
        public void ShouldAggregate()
        {
            List<string> inputValues = new List<string>{
                "stream",
                "all", 
                "the",
                "things",
                "hi",
                "world",
                "kafka",
                "streams",
                "streaming" 
            };

            // We want to compute the sum of the length of words (values) by the first letter (new key) of words.
            Dictionary<string, long> expectedOutput = new Dictionary<string, long>
            {
                {"a", 3L },
                {"t", 9L },
                {"h", 2L },
                {"w", 5L },
                {"k", 5L },
                {"s", 22L}
            };


            // Step 1: Configure and start the processor topology.
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "aggregate-test";
            config.BootstrapServers = "dummy config";

            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>(inputTopicName);

            var aggStream = stream
                .GroupBy((k, v) => (v != null && v.Length > 0) ? v.Substring(0, 1).ToLower() : "")
                .Aggregate(
                    () => 0L,
                    (aggKey, newValue, aggValue) => aggValue + newValue.Length,
                    InMemory<string, long>.As("agg-store").WithValueSerdes<Int64SerDes>()
                  );

            aggStream.ToStream().To(outputTopicName);

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                // Step 2: Setup input and output topics.
                var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);
                var outputTopic = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>(outputTopicName);

                // Step 3: Write the input.
                inputTopic.PipeInputs(inputValues);

                // Step 4: Validate the output.
                var map = outputTopic.ReadKeyValuesToMap();

                Assert.AreEqual(expectedOutput, map);
            }
        }
    }
}