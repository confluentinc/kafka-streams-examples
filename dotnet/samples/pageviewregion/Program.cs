using com.dotnet.samples.avro;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Threading;

namespace PageViewRegion
{
    /// <summary>
    /// Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful
    /// computation, using the generic Avro binding for serdes in Kafka Streams.
    /// <para>
    /// In this example, we join a stream of page views (aka clickstreams) that reads from a topic named
    /// "PageViews" with a user profile table that reads from a topic named "UserProfiles" to compute the
    /// number of page views per user region.
    /// </para>
    /// <para>
    /// Note: The generic Avro binding is used for serialization/deserialization. This means the
    /// appropriate Avro schema files must be provided for each of the "intermediate" Avro classes, i.e.
    /// whenever new types of Avro objects (in the form of GenericRecord) are being passed between
    /// processing steps.
    /// </para>
    /// <para>
    /// HOW TO RUN THIS EXAMPLE
    /// </para>
    /// <para>
    /// 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
    /// </para>
    /// <para>
    /// 2) Create the input/intermediate/output topics used by this example.
    /// <code>
    /// $ bin/kafka-topics --create --topic PageViews \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// $ bin/kafka-topics --create --topic UserProfiles \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// $ bin/kafka-topics --create --topic PageViewsByRegion \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// </code>
    /// Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
    /// <para>
    /// 3) Start this example application either in your IDE or on the command line.
    /// </para>
    /// <para>
    /// 4) Write some input data to the source topics.
    /// The already running example application (step 3) will automatically process this input data and
    /// write the results to the output topic.
    /// </para>
    /// <para>
    /// 5) Inspect the resulting data in the output topic, e.g. via kafka-console-consumer.
    /// </para>
    /// <para>
    /// 6) Once you're done with your experiments, you can stop this example via Ctrl-C. If needed,
    /// also stop the Confluent Schema Registry (Ctrl-C), then stop the Kafka broker (Ctrl-C), and
    /// only then stop the ZooKeeper instance (Ctrl-C).
    /// </para>
    /// </summary>
    class Program
    {
        static readonly string PAGE_VIEW_TOPIC = "PageViews";
        static readonly string USER_PROFILE_TOPIC = "UserProfiles";

        static void Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            // IN PROGRESS
            string bootstrapServers = args.Length > 0 ? args[0] : "localhost:9092";
            string schemaRegistryUrl = args.Length > 1 ? args[1] : "http://localhost:8081";

            var config = new StreamConfig<StringSerDes, Int64SerDes>();
            config.ApplicationId = "pageview-region-lambda-example";
            config.ClientId = "pageview-region-lambda-example-client";
            // Where to find Kafka broker(s).
            config.BootstrapServers = bootstrapServers;
            // Set to earliest so we don't miss any data that arrived in the topics before the process
            // started
            config.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            config.SchemaRegistryUrl = schemaRegistryUrl;
            config.AutoRegisterSchemas = true;

            var t = GetTopology();

            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) =>
            {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }

        static Topology GetTopology()
        {
            var builder = new StreamBuilder();

            var viewsByUser = builder
                .Stream<string, PageView, StringSerDes, SchemaAvroSerDes<PageView>>(PAGE_VIEW_TOPIC)
                .SelectKey((k, v) => v.user);

            var userRegions = builder.Table<string, UserProfile, StringSerDes, SchemaAvroSerDes<UserProfile>>
                (USER_PROFILE_TOPIC, InMemory<string, UserProfile>.As($"{USER_PROFILE_TOPIC}-store"))
                .MapValues((v) => v.region);


            var viewsByRegion = viewsByUser.LeftJoin(userRegions,
                (view, region) =>
                {
                    return new ViewRegion
                    {
                        page = view.url,
                        user = view.user,
                        region = region
                    };
                })
                .Map((user, viewRegion) => KeyValuePair.Create(viewRegion.region, viewRegion))
                .GroupByKey()
                .WindowedBy(HoppingWindowOptions.Of(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1)))
                .Count();

            viewsByRegion
                .ToStream((w, c) => w.ToString())
                .To<StringSerDes, Int64SerDes>("PageViewsByRegion");

            return builder.Build();
        }
    }
}
