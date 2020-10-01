using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace StreamToStreamJoinTests
{
    public class StreamToStreamJoinTest
    {
        static readonly String adImpressionsTopic = "adImpressions";
        static readonly String adClicksTopic = "adClicks";
        static readonly String outputTopic = "output-topic";

        [Test]
        public void ShouldJoinTest()
        {
            // Input 1: Ad impressions
            List<KeyValuePair<String, String>> inputAdImpressions = new List<KeyValuePair<String, String>> {
               KeyValuePair.Create("car-advertisement", "shown"),
               KeyValuePair.Create("newspaper-advertisement", "shown"),
               KeyValuePair.Create("gadget-advertisement", "shown")
             };

            // Input 2: Ad clicks
            List<KeyValuePair<String, String>> inputAdClicks = new List<KeyValuePair<String, String>> {
               KeyValuePair.Create("newspaper-advertisement", "clicked"),
               KeyValuePair.Create("gadget-advertisement", "clicked"),
               KeyValuePair.Create("newspaper-advertisement", "clicked")
             };

            List<KeyValuePair<String, String>> expectedResults = new List<KeyValuePair<String, String>> {
               KeyValuePair.Create("car-advertisement", "shown/not-clicked-yet"),
               KeyValuePair.Create("newspaper-advertisement", "shown/not-clicked-yet"),
               KeyValuePair.Create("gadget-advertisement", "shown/not-clicked-yet"),
               KeyValuePair.Create("newspaper-advertisement", "shown/clicked"),
               KeyValuePair.Create("gadget-advertisement", "shown/clicked"),
               KeyValuePair.Create("newspaper-advertisement", "shown/clicked")
             };

            //
            // Step 1: Configure and start the processor topology.
            //
            var builder = new StreamBuilder();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "stream-stream-join-test";
            config.BootstrapServers = "dummy config";

            var alerts = builder.Stream<string, string>(adImpressionsTopic);
            var incidents = builder.Stream<string, string>(adClicksTopic);

            var impressionsAndClicks = alerts.OuterJoin(incidents,
                (impressionValue, clickValue) =>
                    (clickValue == null) ? impressionValue + "/not-clicked-yet" : impressionValue + "/" + clickValue,
                JoinWindowOptions.Of(TimeSpan.FromSeconds(5)));

            impressionsAndClicks.To(outputTopic);

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                //
                // Step 2: Setup input and output topics.
                //
                var impressionInput = driver.CreateInputTopic<string, string>(adImpressionsTopic);
                var clickInput = driver.CreateInputTopic<string, string>(adClicksTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                //
                // Step 3: Produce some input data to the input topic.
                //
                impressionInput.PipeInputs(inputAdImpressions);
                clickInput.PipeInputs(inputAdClicks);

                //
                // Step 4: Verify the application's output data.
                //
                var map = output.ReadKeyValueList().Select(c => KeyValuePair.Create(c.Message.Key, c.Message.Value));
                Assert.AreEqual(expectedResults, map);
            }
        }
    }
}