using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Threading;


namespace WordCount
{
    /// <summary>
    /// Demonstrates, using the high-level KStream DSL, how to implement the WordCount program that
    /// computes a simple word occurrence histogram from an input text.
    /// <para>
    /// In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of
    /// messages represent lines of text; and the histogram output is written to topic
    /// "streams-wordcount-output", where each record is an updated count of a single word.
    /// </para>
    /// <para>
    /// Note: Before running this example you must 1) create the source topic (e.g. via {@code kafka-topics --create ...}),
    /// then 2) start this example and 3) write some data to the source topic (e.g. via {@code kafka-console-producer}).
    /// Otherwise you won't see any data arriving in the output topic.
    /// </para>
    /// <para>
    /// HOW TO RUN THIS EXAMPLE
    /// </para>
    /// <para>
    /// 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
    /// </para>
    /// <para>
    /// 2) Create the input and output topics used by this example.
    /// <code>
    /// $ bin/kafka-topics --create --topic streams-plaintext-input \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// $ bin/kafka-topics --create --topic streams-wordcount-output \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// </code>
    /// Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
    /// </para>
    /// <para>
    /// 3) Start this example application either in your IDE or on the command line.
    /// </para>
    /// <para>
    /// 4) Write some input data to the source topic "streams-plaintext-input" (e.g. via kafka-console-producer).
    /// The already running example application (step 3) will automatically process this input data and write the
    /// results to the output topic "streams-wordcount-output".
    /// </para>
    /// <code>
    /// # Start the console producer. You can then enter input data by writing some line of text, followed by ENTER:
    /// #
    /// #   hello kafka streams<ENTER>
    /// #   all streams lead to kafka<ENTER>
    /// #   join kafka summit<ENTER>
    /// #
    /// # Every line you enter will become the value of a single Kafka message.
    /// $ bin/kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input
    /// </code>
    /// <para>
    /// 5) Inspect the resulting data in the output topic, e.g. via {@code kafka-console-consumer}.
    /// <code>
    /// $ bin/kafka-console-consumer --topic streams-wordcount-output --from-beginning \
    ///                              --bootstrap-server localhost:9092 \
    ///                              --property print.key=true \
    ///                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
    /// </code>
    /// You should see output data similar to below. Please note that the exact output
    /// sequence will depend on how fast you type the above sentences. If you type them
    /// slowly, you are likely to get each count update, e.g., kafka 1, kafka 2, kafka 3.
    /// If you type them quickly, you are likely to get fewer count updates, e.g., just kafka 3.
    /// This is because the commit interval is set to 10 seconds. Anything typed within
    /// that interval will be compacted in memory.
    /// </para>
    /// <code>
    /// hello    1
    /// kafka    1
    /// streams  1
    /// all      1
    /// streams  2
    /// lead     1
    /// to       1
    /// join     1
    /// kafka    3
    /// summit   1
    /// </code>
    /// 6) Once you're done with your experiments, you can stop this example via Ctrl-C. If needed,
    /// also stop the Kafka broker (Ctrl-C), and only then stop the ZooKeeper instance (Ctrl-C).
    /// </summary>
    class Program
    {
        static readonly String inputTopic = "streams-plaintext-input";
        static readonly String outputTopic = "streams-wordcount-output";

        static void Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            string boostrapserver = args.Length > 0 ? args[0] : "localhost:9092";
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
            // against which the application is run.
            config.ApplicationId = "wordcount-example";
            config.ClientId = "wordcount-example-client";
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

            // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
            // represent lines of text (for the sake of this example, we ignore whatever may be stored
            // in the message keys).  The default key and value serdes will be used.
             IKStream<string, string> textLines = builder.Stream<string, string>(inputTopic);

             IKTable<String, long> wordCounts = textLines
              // Split each text line, by whitespace, into words.  The text lines are the record
              // values, i.e. we can ignore whatever data is in the record keys and thus invoke
              // `flatMapValues()` instead of the more generic `flatMap()`.
              .FlatMapValues(value => value.ToLower().Split(" "))
              // Group the split data by word so that we can subsequently count the occurrences per word.
              // This step re-keys (re-partitions) the input data, with the new record key being the words.
              // Note: No need to specify explicit serdes because the resulting key and value types
              // (String and String) match the application's default serdes.
              .GroupBy((k, w) => w)
              // Count the occurrences of each word (record key).
              .Count();

            // Write the `KTable<String, Long>` to the output topic.
            wordCounts.ToStream().To<StringSerDes, Int64SerDes>(outputTopic);

            return builder.Build();
        }
    }
}
