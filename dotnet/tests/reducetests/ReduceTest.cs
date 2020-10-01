using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using TestUtils;

namespace ReduceTests
{
    public class ReduceTest
    {
        static readonly String inputTopicName = "inputTopic";
        static readonly String outputTopicName = "outputTopic";

        [Test]
        public void ShouldReduceTest()
        {
            List<KeyValuePair<int, String>> inputValues = new List<KeyValuePair<int, String>> {
                  KeyValuePair.Create(456, "stream"),
                  KeyValuePair.Create(123, "hello"),
                  KeyValuePair.Create(123, "world"),
                  KeyValuePair.Create(456, "all"),
                  KeyValuePair.Create(123, "kafka"),
                  KeyValuePair.Create(456, "the"),
                  KeyValuePair.Create(456, "things"),
                  KeyValuePair.Create(123, "streams")
            };

            Dictionary<int, String> expected = new Dictionary<int, String> {
                { 456, "stream all the things"},
                { 123, "hello world kafka streams"}
            };

            //
            // Step 1: Configure and start the processor topology.
            //
            var builder = new StreamBuilder();

            var config = new StreamConfig<Int32SerDes, StringSerDes>();
            config.ApplicationId = "reduce-test";
            config.BootstrapServers = "dummy config";

            builder
                .Stream<int, string>(inputTopicName)
                .GroupByKey()
                .Reduce((v1, v2) => $"{v1} {v2}")
                .ToStream()
                .To(outputTopicName);

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                //
                // Step 2: Setup input and output topics.
                //
                var inputTopic = driver.CreateInputTopic<int, string>(inputTopicName);
                var outputTopic = driver.CreateOuputTopic<int, string>(outputTopicName);

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