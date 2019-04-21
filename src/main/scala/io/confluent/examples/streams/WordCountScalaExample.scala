package io.confluent.examples.streams

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

/**
  * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program that
  * computes a simple word occurrence histogram from an input text.
  * Same as [[WordCountLambdaExample]] but in Scala.
  *
  * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of
  * messages represent lines of text; and the histogram output is written to topic
  * "streams-wordcount-output", where each record is an updated count of a single word, i.e.
  * `word (String) -> currentCount (Long)`.
  *
  * Note: Before running this example you must 1) create the source topic (e.g. via `kafka-topics --create ...`),
  * then 2) start this example and 3) write some data to the source topic (e.g. via `kafka-console-producer`).
  * Otherwise you won't see any data arriving in the output topic.
  *
  *
  * HOW TO RUN THIS EXAMPLE
  *
  * 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
  *
  * 2) Create the input and output topics used by this example.
  *
  * {{{
  * $ bin/kafka-topics --create --topic streams-plaintext-input --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * $ bin/kafka-topics --create --topic streams-wordcount-output --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * }}}
  *
  * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be `bin/kafka-topics.sh ...`.
  *
  * 3) Start this example application either in your IDE or on the command line.
  *
  * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
  * Once packaged you can then run:
  *
  * {{{
  * $ java -cp target/kafka-streams-examples-5.2.1-standalone.jar io.confluent.examples.streams.WordCountLambdaExample
  * }}}
  *
  * 4) Write some input data to the source topic "streams-plaintext-input" (e.g. via `kafka-console-producer`).
  * The already running example application (step 3) will automatically process this input data and write the
  * results to the output topic "streams-wordcount-output".
  *
  * {{{
  * # Start the console producer. You can then enter input data by writing some line of text, followed by ENTER:
  * #
  * #   hello kafka streams<ENTER>
  * #   all streams lead to kafka<ENTER>
  * #   join kafka summit<ENTER>
  * #
  * # Every line you enter will become the value of a single Kafka message.
  * $ bin/kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input
  * }}}
  *
  * 5) Inspect the resulting data in the output topic, e.g. via `kafka-console-consumer`.
  * {{{
  * $ bin/kafka-console-consumer --topic streams-wordcount-output --from-beginning \
  *                              --bootstrap-server localhost:9092 \
  *                              --property print.key=true \
  *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
  * }}}
  * You should see output data similar to below. Please note that the exact output
  * sequence will depend on how fast you type the above sentences. If you type them
  * slowly, you are likely to get each count update, e.g., kafka 1, kafka 2, kafka 3.
  * If you type them quickly, you are likely to get fewer count updates, e.g., just kafka 3.
  * This is because the commit interval is set to 10 seconds. Anything typed within
  * that interval will be compacted in memory.
  *
  * {{{
  * hello    1
  * kafka    1
  * streams  1
  * all      1
  * streams  2
  * lead     1
  * to       1
  * join     1
  * kafka    3
  * summit   1
  * }}}
  *
  * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`. If needed,
  * also stop the Kafka broker (`Ctrl-C`), and only then stop the ZooKeeper instance (`Ctrl-C`).
  **/
object WordCountScalaExample extends App {

  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  val builder = new StreamsBuilder()
  val textLines: KStream[String, String] = builder.stream[String, String]("streams-plaintext-input")
  val wordCounts: KTable[String, Long] = textLines
    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
    .groupBy((_, word) => word)
    .count()
  wordCounts.toStream.to("streams-wordcount-output")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  // Always (and unconditionally) clean local state prior to starting the processing topology.
  // We opt for this unconditional call here because this will make it easier for you to play around with the example
  // when resetting the application for doing a re-run (via the Application Reset Tool,
  // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
  //
  // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
  // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
  // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
  // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
  // See `ApplicationResetExample.java` for a production-like example.
  streams.cleanUp()

  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

}
