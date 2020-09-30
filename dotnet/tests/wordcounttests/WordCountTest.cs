using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using System.Collections.Generic;
using System.Linq;

namespace WordCountTests
{
    /// <summary>
    /// End-to-end integration test based on WordCount samples.
    /// </summary>
    public class WordCountTest
    {
        private static readonly string inputTopicName = "inputTopic";
        private static readonly string outputTopicName = "outputTopic";

        [Test]
        public void ShouldCountWords()
        {
            List<string> inputValues = new List<string> {
                "Hello Kafka Streams",
                "All streams lead to Kafka",
                "Join Kafka Summit",
            };

            List<KeyValuePair<string, long>> expected = new List<KeyValuePair<string, long>>
            {
                KeyValuePair.Create("hello", 1L ),
                KeyValuePair.Create("kafka", 1L ),
                KeyValuePair.Create("streams", 1L ),
                KeyValuePair.Create("all", 1L ),
                KeyValuePair.Create("streams", 2L ),
                KeyValuePair.Create("lead", 1L ),
                KeyValuePair.Create("to", 1L ),
                KeyValuePair.Create("kafka", 2L ),
                KeyValuePair.Create("join", 1L ),
                KeyValuePair.Create("kafka", 3L ),
                KeyValuePair.Create("summit", 1L )
            };

            // Step 1: Configure and start the processor topology.
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "wordcount-lambda-integration-test";
            config.BootstrapServers = "dummy config";

            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>(inputTopicName);

            var wordCounts = stream
                .FlatMapValues(value => value.ToLower().Split(" "))
                .GroupBy((k, word) => word)
                .Count();

            wordCounts.ToStream().To(outputTopicName);

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                // Step 2: Setup input and output topics.
                var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);
                var outputTopic = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>(outputTopicName);

                // Step 3: Write the input.
                inputTopic.PipeInputs(inputValues);

                // Step 4: Validate the output.
                var outputs = outputTopic
                                .ReadKeyValueList()
                                .Select(c => KeyValuePair.Create(c.Message.Key, c.Message.Value))
                                .ToList();

                Assert.AreEqual(expected, outputs);
            }
        }
    }
}