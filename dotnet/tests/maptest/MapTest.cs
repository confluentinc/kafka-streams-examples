using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MapTests
{
    public class MapTest
    {
        static readonly String inputTopicName = "inputTopic";
        static readonly String outputTopicName = "outputTopic";

        [Test]
        public void ShouldMapValues()
        {
            List<String> inputValues = new List<String> { "hello", "world" };
            List<String> expectedValues = new List<String> { "HELLO", "WORLD" };

            //
            // Step 1: Configure and start the processor topology.
            //
            var builder = new StreamBuilder();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "map-test";
            config.BootstrapServers = "dummy config";

            var stream = builder.Stream<string, string>(inputTopicName);
            var uppercased = stream.MapValues((v) => v.ToUpper());
            uppercased.To(outputTopicName);

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                //
                // Step 2: Setup input and output topics.
                //
                var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);
                var outputTopic = driver.CreateOuputTopic<string, string>(outputTopicName);

                //
                // Step 3: Produce some input data to the input topic.
                //
                inputTopic.PipeInputs(inputValues);

                //
                // Step 4: Verify the application's output data.
                //
                Assert.AreEqual(expectedValues, outputTopic.ReadValueList());

            }

        }

        [Test]
        public void ShouldMap()
        {
            List<KeyValuePair<String, String>> inputValues = new List<KeyValuePair<String, String>> {
                KeyValuePair.Create("120", "ok" ),
                KeyValuePair.Create("240", "ko" )
            };

            List<KeyValuePair<String, String>> expected = new List<KeyValuePair<String, String>> {
                KeyValuePair.Create("1", "OK" ),
                KeyValuePair.Create("2", "KO" )
            };

            //
            // Step 1: Configure and start the processor topology.
            //
            var builder = new StreamBuilder();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "map-test";
            config.BootstrapServers = "dummy config";

            var stream = builder.Stream<string, string>(inputTopicName);
            var uppercased = stream.Map((k,v) => KeyValuePair.Create(k.Substring(0, 1),  v.ToUpper()));
            uppercased.To(outputTopicName);

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                //
                // Step 2: Setup input and output topics.
                //
                var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);
                var outputTopic = driver.CreateOuputTopic<string, string>(outputTopicName);

                //
                // Step 3: Produce some input data to the input topic.
                //
                inputTopic.PipeInputs(inputValues);

                //
                // Step 4: Verify the application's output data.
                //
                Assert.AreEqual(expected, outputTopic.ReadKeyValueList().Select(c => KeyValuePair.Create(c.Message.Key, c.Message.Value)));
            }
        }
    }
}