using NUnit.Framework;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using TestUtils;

namespace StreamToTableJoinTests
{
    internal class RegionWithClicks
    {

        public RegionWithClicks(string p, long clicks)
        {
            Region = p;
            Clicks = clicks;
        }

        public string Region { get; set; }
        public long Clicks { get; set; }
    }

    public class StreamToTableJoinTest
    {
        private static readonly String userClicksTopic = "user-clicks";
        private static readonly String userRegionsTopic = "user-regions";
        private static readonly String outputTopic = "output-topic";

        [Test]
        public void ShouldJoinTest()
        {
            // Input 1: Clicks per user (multiple records allowed per user).
            List<KeyValuePair<string, long>> userClicks = new List<KeyValuePair<string, long>> {
                KeyValuePair.Create("alice", 13L),
                KeyValuePair.Create("bob", 4L),
                KeyValuePair.Create("chao", 25L),
                KeyValuePair.Create("bob", 19L),
                KeyValuePair.Create("dave", 56L),
                KeyValuePair.Create("eve", 78L),
                KeyValuePair.Create("alice", 40L),
                KeyValuePair.Create("fang", 99L)
            };

            // Input 2: Region per user (multiple records allowed per user).
            List<KeyValuePair<string, string>> userRegions = new List<KeyValuePair<string, string>> {
                KeyValuePair.Create("alice", "asia"),   /* Alice lived in Asia originally... */
                KeyValuePair.Create("bob", "americas"),
                KeyValuePair.Create("chao", "asia"),
                KeyValuePair.Create("dave", "europe"),
                KeyValuePair.Create("alice", "europe"), /* ...but moved to Europe some time later. */
                KeyValuePair.Create("eve", "americas"),
                KeyValuePair.Create("fang", "asia")
            };

            Dictionary<String, long> expectedClicksPerRegion = new Dictionary<String, long>
            {
                { "americas", 101L },
                { "europe", 109L },
                { "asia", 124L },
            };

            //
            // Step 1: Configure and start the processor topology.
            //
            var builder = new StreamBuilder();

            var config = new StreamConfig<StringSerDes, Int64SerDes>();
            config.ApplicationId = "stream-stream-join-test";
            config.BootstrapServers = "dummy config";

            // This KStream contains information such as "alice" -> 13L.
            //
            // Because this is a KStream ("record stream"), multiple records for the same user will be
            // considered as separate click-count events, each of which will be added to the total count.
            var userClicksStream = builder.Stream<string, long>(userClicksTopic);

            // This KTable contains information such as "alice" -> "europe".
            //
            // Because this is a KTable ("changelog stream"), only the latest value (here: region) for a
            // record key will be considered at the time when a new user-click record (see above) is
            // received for the `leftJoin` below.  Any previous region values are being considered out of
            // date.  This behavior is quite different to the KStream for user clicks above.
            //
            // For example, the user "alice" will be considered to live in "europe" (although originally she
            // lived in "asia") because, at the time her first user-click record is being received and
            // subsequently processed in the `leftJoin`, the latest region update for "alice" is "europe"
            // (which overrides her previous region value of "asia").
            var userRegionsTable = builder.Table<string, string, StringSerDes, StringSerDes>(userRegionsTopic);

            // Compute the number of clicks per region, e.g. "europe" -> 13L.
            //
            // The resulting KTable is continuously being updated as new data records are arriving in the
            // input KStream `userClicksStream` and input KTable `userRegionsTable`.
            var clicksPerRegion = userClicksStream
                                            .LeftJoin(userRegionsTable,
                                                (clicks, region) => new RegionWithClicks((region == null ? "UNKNOWN" : region), clicks))
                                            .Map((user, region) => KeyValuePair.Create(region.Region, region.Clicks))
                                            .GroupByKey()
                                            .Reduce((v1, v2) => v1 + v2);

            clicksPerRegion
                .ToStream()
                .To<StringSerDes, Int64SerDes>(outputTopic);



            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                //
                // Step 2: Setup input and output topics.
                //
                var regionInput = driver.CreateInputTopic<string, string, StringSerDes, StringSerDes>(userRegionsTopic);
                var clickInput = driver.CreateInputTopic<string, long>(userClicksTopic);
                var output = driver.CreateOuputTopic<string, long>(outputTopic);

                //
                // Step 3: Produce some input data to the input topic.
                //
                regionInput.PipeInputs(userRegions);
                clickInput.PipeInputs(userClicks);

                //
                // Step 4: Verify the application's output data.
                //
                var map = output.ReadKeyValuesToMap();
                Assert.AreEqual(expectedClicksPerRegion, map);
            }
        }
    }
}