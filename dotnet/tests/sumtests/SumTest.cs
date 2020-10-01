using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;

namespace SumTests
{
    public class Void
    {

    }

    public class VoidSerDes : AbstractSerDes<Void>
    {
        public override Void Deserialize(byte[] data, SerializationContext context)
        {
            return new Void();
        }

        public override byte[] Serialize(Void data, SerializationContext context)
        {
            return new byte[0];
        }
    }

    public class Tests
    {
        static readonly String inputTopicName = "inputTopic";
        static readonly String outputTopicName = "outputTopic";

        [Test]
        public void ShouldSum()
        {
            List<int> inputValues = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

            List<int> expected = new List<int>
            {
                1,
                4,
                9,
                16,
                25
            };

            //
            // Step 1: Configure and start the processor topology.
            //
            var builder = new StreamBuilder();

            var config = new StreamConfig<Int32SerDes, Int32SerDes>();
            config.ApplicationId = "sum-test";
            config.BootstrapServers = "dummy config";

            builder
                .Stream<Void, int, VoidSerDes, Int32SerDes>(inputTopicName)
                .Filter((k, v) => v % 2 != 0)
                .SelectKey((k, v) => 1)
                .GroupByKey()
                .Reduce((v1, v2) => v1 + v2)
                .ToStream().To(outputTopicName);


            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                //
                // Step 2: Setup input and output topics.
                //
                var inputTopic = driver.CreateInputTopic<Void, int>(inputTopicName);
                var outputTopic = driver.CreateOuputTopic<int, int>(outputTopicName);

                //
                // Step 3: Produce some input data to the input topic.
                //
                inputTopic.PipeInputs(inputValues);

                //
                // Step 4: Verify the application's output data.
                //
                var map = outputTopic.ReadValueList();
                Assert.AreEqual(expected, map);
            }
        }
    }
}