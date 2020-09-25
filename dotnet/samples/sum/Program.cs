using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Threading;

namespace Sum
{
    /// <summary>
    /// Demonstrates how to use `reduce` to sum numbers.
    /// <para>
    /// HOW TO RUN THIS EXAMPLE
    /// </para>
    /// <para>
    /// 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
    /// </para>
    /// <para>
    /// 2) Create the input and output topics used by this example.
    /// </para>
    /// <code>
    /// $ bin/kafka-topics --create --topic numbers-topic \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// $ bin/kafka-topics --create --topic sum-of-odd-numbers-topic \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// </code>
    /// Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code `bin/kafka-topics.sh ...}.
    /// <para>
    /// 3) Start this example application either in your IDE or on the command line.
    /// </para>
    /// <para>
    /// 4) Write some input data to the source topic. The
    /// already running example application (step 3) will automatically process this input data and write
    /// the results to the output topic.
    /// </para>
    /// <para>
    /// 5) Inspect the resulting data in the output topics, e.g. via kafka-console-consumer.
    /// <code>
    /// $ bin/kafka-console-consumer --topic sum-of-odd-numbers-topic --from-beginning \
    ///                              --bootstrap-server localhost:9092 \
    ///                              --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer
    /// </code>
    /// </para>
    /// <para>
    /// 6) Once you're done with your experiments, you can stop this example via Ctrl-C. If needed,
    /// also stop the Kafka broker (Ctrl-C}), and only then stop the ZooKeeper instance (Ctrl-C).
    /// </para>
    /// </summary>
    class Program
    {
        static readonly String SUM_OF_ODD_NUMBERS_TOPIC = "sum-of-odd-numbers-topic";
        static readonly String NUMBERS_TOPIC = "numbers-topic";

        static void Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            string boostrapserver = args.Length > 0 ? args[0] : "localhost:9092";
            var config = new StreamConfig<Int32SerDes, Int32SerDes>();
            // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
            // against which the application is run.
            config.ApplicationId = "sum-example";
            config.ClientId = "sum-example-client";
            // Where to find Kafka broker(s).
            config.BootstrapServers = boostrapserver;
            config.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 10 * 1000;

            Topology t = GetTopology();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }

        static Topology GetTopology()
        {
            StreamBuilder builder = new StreamBuilder();
            // We assume the input topic contains records where the values are Integers.
            // We don't really care about the keys of the input records;  for simplicity, we assume them
            // to be Integers, too, because we will re-key the stream later on, and the new key will be
            // of type Integer.
            IKStream<byte[], int> input = builder.Stream<byte[], int, ByteArraySerDes, Int32SerDes>(NUMBERS_TOPIC);

            IKTable<int, int> sumOfOddNumbers = input
             // We are only interested in odd numbers.
             .Filter((k, v) =>v % 2 != 0)
             // We want to compute the total sum across ALL numbers, so we must re-key all records to the
             // same key.  This re-keying is required because in Kafka Streams a data record is always a
             // key-value pair, and KStream aggregations such as `reduce` operate on a per-key basis.
             // The actual new key (here: `1`) we pick here doesn't matter as long it is the same across
             // all records.
             .SelectKey((k, v) => 1)
             // no need to specify explicit serdes because the resulting key and value types match our default serde settings
             .GroupByKey()
             // Add the numbers to compute the sum.
             .Reduce((v1, v2) => v1 + v2);

            sumOfOddNumbers.ToStream().To(SUM_OF_ODD_NUMBERS_TOPIC);

            return builder.Build();
        }
    }
}
