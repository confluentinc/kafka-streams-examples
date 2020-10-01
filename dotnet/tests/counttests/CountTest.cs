using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using TestUtils;

namespace CountTests
{
    public class CountTest
    {
        static readonly String inputTopicName = "inputTopic";
        static readonly String outputTopicName = "outputTopic";

        [Test]
        public void ShouldCountTest()
        {
            List<KeyValuePair<string, String>> inputValues = new List<KeyValuePair<string, String>> {
                  KeyValuePair.Create("alice", "asia"),
                  KeyValuePair.Create("bob", "europe"),
                  KeyValuePair.Create("alice", "europe"),
                  KeyValuePair.Create("bob", "asia")
            };

            Dictionary<string, long> expected = new Dictionary<string, long> {
                { "europe", 1}, // in the end, Alice is in europe
                { "asia", 1} // in the end, Bob is in asia
            };

            //
            // Step 1: Configure and start the processor topology.
            //
            var builder = new StreamBuilder();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "count-test";
            config.BootstrapServers = "dummy config";

            var usersPerRegionTable = builder
                .Table<string, string>(inputTopicName)
                .GroupBy((k, v) => KeyValuePair.Create(v, v))
                .Count();

            usersPerRegionTable
                .ToStream()
                .To(outputTopicName);

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                //
                // Step 2: Setup input and output topics.
                //
                var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);
                var outputTopic = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>(outputTopicName);

                //
                // Step 3: Produce some input data to the input topic.
                //
                inputTopic.PipeInputs(inputValues);

                //
                // Step 4: Verify the application's output data.
                //
                var map = outputTopic.ReadKeyValuesToMap();
                Assert.AreEqual(expected, map);
            }
        }
    }
}