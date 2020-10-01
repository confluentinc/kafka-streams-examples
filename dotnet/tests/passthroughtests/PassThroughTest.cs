using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;


namespace PassThroughTests
{
    public class PassThroughTest
    {
        static readonly String inputTopicName = "inputTopic";
        static readonly String outputTopicName = "outputTopic";

        [Test]
        public void ShouldThrough()
        {
            List<String> inputValues = new List<String> { "hello", "world" };
            List<String> expectedValues = new List<String> { "HELLO", "WORLD" };

            //
            // Step 1: Configure and start the processor topology.
            //
            var builder = new StreamBuilder();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "through-test";
            config.BootstrapServers = "dummy config";

            builder.Stream<string, string>(inputTopicName).To(outputTopicName);

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
                Assert.AreEqual(inputValues, outputTopic.ReadValueList());
            }
        }
    }
}