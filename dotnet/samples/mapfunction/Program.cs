using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Threading;

namespace MapFunction
{
    /// <summary>
    /// Demonstrates how to perform simple, state-less transformations via map functions.
    /// <para>
    /// Use cases include e.g. basic data sanitization, data anonymization by obfuscating sensitive data
    /// fields (such as personally identifiable information aka PII).  This specific example reads
    /// incoming text lines and converts each text line to all-uppercase.
    /// </para>
    /// <para>
    /// HOW TO RUN THIS EXAMPLE
    /// </para>
    /// <para>
    /// 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
    /// </para>
    /// <para>
    /// 2) Create the input and output topics used by this example.
    /// </para>
    /// <para>
    /// <code>
    /// $ bin/kafka-topics --create --topic TextLinesTopic \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// $ bin/kafka-topics --create --topic UppercasedTextLinesTopic \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// $ bin/kafka-topics --create --topic OriginalAndUppercasedTopic \
    ///                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
    /// </code>
    /// Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
    /// <para>
    /// 3) Start this example application either in your IDE or on the command line.
    /// </para>
    /// <para>
    /// 4) Write some input data to the source topic (e.g. via kafka-console-producer). The already
    /// running example application (step 3) will automatically process this input data and write the
    /// results to the output topics.
    /// </para>
    /// <code>
    /// # Start the console producer.  You can then enter input data by writing some line of text, followed by ENTER:
    /// #
    /// #   hello kafka streams<ENTER>
    /// #   all streams lead to kafka<ENTER>
    /// #
    /// # Every line you enter will become the value of a single Kafka message.
    /// $ bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic
    /// </code>
    /// <para>
    /// 5) Inspect the resulting data in the output topics, e.g. via {@code kafka-console-consumer}.
    /// </para>
    /// <code>
    /// $ bin/kafka-console-consumer --topic UppercasedTextLinesTopic --from-beginning \
    ///                              --bootstrap-server localhost:9092
    /// $ bin/kafka-console-consumer --topic OriginalAndUppercasedTopic --from-beginning \
    ///                              --bootstrap-server localhost:9092 --property print.key=true
    /// </code>
    /// You should see output data similar to
    /// <code>
    /// HELLO KAFKA STREAMS
    /// ALL STREAMS LEAD TO KAFKA
    /// </code>
    /// 6) Once you're done with your experiments, you can stop this example via Ctrl-C.
    ///
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            string boostrapserver = args.Length > 0 ? args[0] : "localhost:9092";
            var config = new StreamConfig<ByteArraySerDes, StringSerDes>();
            // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
            // against which the application is run.
            config.ApplicationId = "map-function-example";
            config.ClientId = "map-function-example-client";
            // Where to find Kafka broker(s).
            config.BootstrapServers = boostrapserver;

            // In the subsequent lines we define the processing topology of the Streams application.
            var builder = new StreamBuilder();

            // Read the input Kafka topic into a KStream instance
            var textLines = builder.Stream<byte[], string>("TextLinesTopic");
            // Variant 1: using `MapValues`
            var upperCase = textLines.MapValues(v => v.ToUpper());

            // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
            // In this case we can rely on the default serializers for keys and values because their data
            // types did not change, i.e. we only need to provide the name of the output topic.
            upperCase.To("UppercasedTextLinesTopic");

            // Variant 2: using `map`, modify value only (equivalent to variant 1)
            var uppercasedWithMap = textLines.Map((key, value) => KeyValuePair.Create(key, value.ToUpper()));

            // Variant 3: using `map`, modify both key and value
            //
            // Note: Whether, in general, you should follow this artificial example and store the original
            //       value in the key field is debatable and depends on your use case.  If in doubt, don't
            //       do it.
           var originalAndUppercased = textLines.Map((key, value) => KeyValuePair.Create(value, value.ToUpper()));

            // Write the results to a new Kafka topic "OriginalAndUppercasedTopic".
            //
            // In this case we must explicitly set the correct serializers because the default serializers
            // (cf. streaming configuration) do not match the type of this particular KStream instance.
            originalAndUppercased.To<StringSerDes, StringSerDes>("OriginalAndUppercasedTopic");

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }
    }
}
