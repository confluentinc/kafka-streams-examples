/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


  import org.apache.kafka.common.serialization.Serde;
  import org.apache.kafka.common.serialization.Serdes;
  import org.apache.kafka.streams.*;
  import org.apache.kafka.streams.kstream.*;

  import java.text.SimpleDateFormat;
  import java.util.Properties;
  import java.util.TimeZone;

  /**
   * Demonstrates converting Unix Epoch Seconds String to a timestamp format in Kafka Streams Application. We do not have readily available Streams functions like in KSQL to achieve this.
   * However, with the help of Built-in JAVA classes and methods, we can convert Unix Epoch Seconds String to a timestamp format.
   * <br>
   * HOW TO RUN THIS EXAMPLE
   * <p>
   * 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
   * <p>
   * 2) Create the input and output topics used by this example.
   * <pre>
   * {@code
   * $ bin/kafka-topics --create --topic InputTopic \
   *                    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   * $ bin/kafka-topics --create --topic OutputTopic \
   *                    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   * }</pre>
   * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
   * <p>
   * 3) Start this example application either in your IDE or on the command line.
   * <p>
   * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
   * Once packaged you can then run:
   * <pre>
   * {@code
   * $ java -cp target/kafka-streams-examples-7.8.0-0-standalone.jar io.confluent.examples.streams.UnixEpochCoverterExample
   * }
   * </pre>
   * 4) Write some input data to the source topics (e.g. via {@code kafka-console-producer}). The already
   * running example application (step 3) will automatically process this input data and write the
   * results to the output topic.
   * <pre>
   * {@code
   * # Start the console producer, then input some example data records. The input data you enter
   * # should be in the form of Key,EpochTimestampSeconds<ENTER>
   * #
   * # abc,1701855889
   * #
   * # Here, the part before the comma will become the message key and the part after the comma will
   * # become the message value.
   * $ bin/kafka-console-producer --bootstrap-server localhost:9092 --topic InputTopic \
   *                              --property parse.key=true --property key.separator=,
   * }</pre>
   * 5) Inspect the resulting data in the output topics, e.g. via {@code kafka-console-consumer}.
   * <pre>
   * {@code
   * $ bin/kafka-console-consumer --topic OutputTopic --from-beginning \
   *                              --bootstrap-server localhost:9092 \
   *                              --property print.key=true
   *
   * }</pre>
   * You should see output data similar to:
   * <pre>
   * {@code
   * abc 2023-12-06 05:44:49 GMT-04:00  #This is because we are converting unix epoch seconds to timestamp format
   *
   * }</pre>
   * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
   * also stop the Kafka broker ({@code Ctrl-C}), and only then stop the ZooKeeper instance ({@code Ctrl-C}).
   */

  public class UnixEpochCoverterExample {
      public static void main(final String[] args) {
          final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
          final Properties streamsConfiguration = new Properties();
          // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
          // against which the application is run.
          streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
          streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-lambda-example-client");
          // Where to find Kafka broker(s).
          streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          // Specify default (de)serializers for record keys and for record values.
          streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
          streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
          //Use SDF to specify the time format and timezone
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
          sdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
          final Serde<String> stringSerde = Serdes.String();
          final StreamsBuilder builder = new StreamsBuilder();
          final KStream<String, String> srcStream = builder.stream("InputTopic", Consumed.with(stringSerde, stringSerde));
          //Use mapValues to transform each record's value from integer to sdf format.
          final KStream<String, String> sinkStream = srcStream.mapValues(v -> sdf.format(Integer.parseInt(v)*1000L));
          sinkStream.to("OutputTopic");
          final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
          streams.cleanUp();
          streams.start();
          // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
          Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      }
  }
