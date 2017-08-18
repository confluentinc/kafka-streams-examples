# Kafka Streams examples [![Build Status](https://travis-ci.org/confluentinc/examples.svg?branch=3.2.x)](https://travis-ci.org/confluentinc/examples)

This sub-folder contains code examples that demonstrate how to implement real-time applications and event-driven
microservices using the Streams API of [Apache Kafka](http://kafka.apache.org/) aka Kafka Streams.

For more information take a look at the
[**latest Confluent documentation on the Kafka Streams API**](http://docs.confluent.io/current/streams/), notably the
[**Developer Guide**](http://docs.confluent.io/current/streams/developer-guide.html).


---
Table of Contents

* [Available examples](#available-examples)
    * [Java](#examples-java)
    * [Scala](#examples-scala)
* [Requirements](#requirements)
    * [Apache Kafka](#requirements-kafka)
    * [Confluent Platform](#requirements-confluent-platform)
    * [Java](#requirements-java)
    * [Scala](#requirements-scala)
* [Packaging and running the examples](#packaging-and-running)
* [Development](#development)
* [Version Compatibility Matrix](#version-compatibility)
* [Where to find help](#help)

---


<a name="available-examples"/>

# Available examples

This repository has several branches to help you find the correct code examples for the version of Apache Kafka and/or
Confluent Platform that you are using.  See [Version Compatibility Matrix](#version-compatibility) below for details.

There are two kinds of examples:

* **Examples under [src/main/](src/main/)**: These examples are short and concise.  Also, you can interactively
  test-drive these examples, e.g. against a local Kafka cluster.  If you want to actually run these examples, then you
  must first install and run Apache Kafka and friends, which we describe in section
  [Packaging and running the examples](#packaging-and-running).  Each example also states its exact requirements and
  instructions at the very top.
* **Examples under [src/test/](src/test/)**: These examples are a bit longer because they implement integration tests
  that demonstrate end-to-end data pipelines.  Here, we use a testing framework to automatically spawn embedded Kafka
  clusters, feed input data to them (using the standard Kafka producer client), process the data using Kafka Streams,
  and finally read and verify the output results (using the standard Kafka consumer client).
  These examples are also a good starting point to learn how to implement your own end-to-end integration tests.


<a name="examples-java"/>

## Java

> Note: We use the label "Lambda" to denote examples that make use of lambda expressions and thus require Java 8+.

* [WordCountLambdaExample](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java)
  -- demonstrates, using the Kafka Streams DSL, how to implement the WordCount program that computes a simple word
  occurrence histogram from an input text.
* [MapFunctionLambdaExample](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java)
  -- demonstrates how to perform stateless transformations via map functions, using the Kafka Streams DSL
  (see also the Scala variant
  [MapFunctionScalaExample](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala))
* [SessionWindowsExample](src/main/java/io/confluent/examples/streams/SessionWindowsExample.java)
  -- demonstrates how to perform user behavior analysis through sessionization of user events
* [SumLambdaExample](src/main/java/io/confluent/examples/streams/SumLambdaExample.java)
  -- demonstrates how to perform stateful transformations via `reduce`, using the Kafka Streams DSL
* [PageViewRegionLambdaExample](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java)
  -- demonstrates how to perform a join between a `KStream` and a `KTable`, i.e. an example of a stateful computation
    * Variant: [PageViewRegionExample](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java),
      which implements the same example but without lambda expressions and thus works with Java 7+.
* Working with data in Apache Avro format (see also the end-to-end demos under integration tests below):
    * Generic Avro:
      [PageViewRegionLambdaExample](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java)
      (Java 8+) and
      [PageViewRegionExample](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) (Java 7+)
    * Specific Avro:
      [WikipediaFeedAvroLambdaExample](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroLambdaExample.java)
      (Java 8+) and
      [WikipediaFeedAvroExample](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroExample.java) (Java 7+)
* [SecureKafkaStreamsExample](src/main/java/io/confluent/examples/streams/SecureKafkaStreamsExample.java) (Java 7+)
  -- demonstrates how to configure Kafka Streams for secure stream processing (here: encrypting data-in-transit
  and enabling client authentication so that the Kafka Streams application authenticates itself to the Kafka brokers)
* [StateStoresInTheDSLIntegrationTest](src/test/java/io/confluent/examples/streams/StateStoresInTheDSLIntegrationTest.java) (Java 8+)
  -- demonstrates how to use state stores in the Kafka Streams DSL
* [WordCountInteractiveQueriesExample](src/main/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExample.java) (Java 8+)
  -- demonstrates the Interactive Queries feature to locate and query state stores of a Kafka Streams application
  from other applications; here, we opted to use a REST API to implement the required RPC layer to allow applications to
  talk to each other
* [KafkaMusicExample](src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java) (Java 8+)
  -- demonstrates the building of a simple music charts application. Uses the Interactive Queries feature to query the state stores to get
  the latest top five songs. Demonstrates locating the KafkaStreams instance for a store and key and retrieving the values via a REST API
* [HandlingCorruptedInputRecordsIntegrationTest](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java)
  (Java 8+) -- demonstrates how to handle corrupt input records (think: poison pill messages)
* [MixAndMatchLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/MixAndMatchLambdaIntegrationTest.java)
  (Java 8+) -- demonstrates how to mix and match the DSL and the Processor API via `KStream#transform()` and
  `KStream#process()`, which allow you to include custom `Transformer` and `Processor` implementations, respectively,
  within topologies defined via the DSL
* [ApplicationResetExample](src/main/java/io/confluent/examples/streams/ApplicationResetExample.java) (Java 8+)
  -- demonstrates the usage of the application reset tool (`bin/kafka-streams-application-reset`)
* [GlobalKTablesExample](src/main/java/io/confluent/examples/streams/GlobalKTablesExample.java) (Java 8+)
  -- demonstrates joining between `KStream` and `GlobalKTable`.
* And [further examples](src/main/java/io/confluent/examples/streams/).

We also provide several **integration tests**, which demonstrate end-to-end data pipelines.  Here, we spawn embedded Kafka
clusters and the [Confluent Schema Registry](https://github.com/confluentinc/schema-registry), feed input data to them
(using the standard Kafka producer client), process the data using Kafka Streams, and finally read and verify the output
results (using the standard Kafka consumer client).

> Tip: Run `mvn test` to launch the integration tests.

* [WordCountLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java)
* [WordCountInteractiveQueriesExampleTest](src/test/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExampleTest.java)
* [EventDeduplicationLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java)
* [GlobalKTableExampleTest](src/test/java/io/confluent/examples/streams/GlobalKTablesExampleTest.java)
* [HandlingCorruptedInputRecordsIntegrationTest](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java)
* [KafkaMusicExampleTest](src/test/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExampleTest.java)
* [MapFunctionLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/MapFunctionLambdaIntegrationTest.java)
* [MixAndMatchLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/MixAndMatchLambdaIntegrationTest.java)
* [PassThroughIntegrationTest](src/test/java/io/confluent/examples/streams/PassThroughIntegrationTest.java)
* [SessionWindowsExampleTest](src/test/java/io/confluent/examples/streams/SessionWindowsExampleTest.java)
* [SumLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/SumLambdaIntegrationTest.java)
* [StreamToStreamJoinIntegrationTest](src/test/java/io/confluent/examples/streams/StreamToStreamJoinIntegrationTest.java)
* [StreamToTableJoinIntegrationTest](src/test/java/io/confluent/examples/streams/StreamToTableJoinIntegrationTest.java)
* [TableToTableJoinIntegrationTest](src/test/java/io/confluent/examples/streams/TableToTableJoinIntegrationTest.java)
* [UserCountsPerRegionLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/UserCountsPerRegionLambdaIntegrationTest.java)
* [GenericAvroIntegrationTest](src/test/java/io/confluent/examples/streams/GenericAvroIntegrationTest.java)
* [SpecificAvroIntegrationTest](src/test/java/io/confluent/examples/streams/SpecificAvroIntegrationTest.java)
* [ValidateStateWithInteractiveQueriesLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/ValidateStateWithInteractiveQueriesLambdaIntegrationTest.java)


<a name="examples-scala"/>

## Scala

* [MapFunctionScalaExample](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala)
  -- demonstrates how to perform simple, state-less transformations via map functions, using the Kafka Streams DSL
  (see also the Java variant
  [MapFunctionLambdaExample](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java))

We also provide several **integration tests**, which demonstrate end-to-end data pipelines.  Here, we spawn embedded Kafka
clusters and the [Confluent Schema Registry](https://github.com/confluentinc/schema-registry), feed input data to them
(using the standard Kafka producer client), process the data using Kafka Streams, and finally read and verify the output
results (using the standard Kafka consumer client).

> Tip: Run `mvn test` to launch the integration tests.

* [StreamToTableJoinScalaIntegrationTest](src/test/scala/io/confluent/examples/streams/StreamToTableJoinScalaIntegrationTest.scala)
* [WordCountScalaIntegrationTest](src/test/scala/io/confluent/examples/streams/WordCountScalaIntegrationTest.scala)
* [GenericAvroScalaIntegrationTest](src/test/scala/io/confluent/examples/streams/GenericAvroScalaIntegrationTest.scala)
* [ProbabilisticCountingScalaIntegrationTest](src/test/scala/io/confluent/examples/streams/ProbabilisticCountingScalaIntegrationTest.scala)
  -- demonstrates how to probabilistically count items in an input stream by implementing a custom state store
  ([CMSStore](src/main/scala/io/confluent/examples/streams/algebird/CMSStore.scala)) that is backed by a
  [Count-Min Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) data structure (with the CMS implementation
  of [Twitter Algebird](https://github.com/twitter/algebird))
* [SpecificAvroScalaIntegrationTest](src/test/scala/io/confluent/examples/streams/SpecificAvroScalaIntegrationTest.scala)


<a name="requirements"/>

# Requirements

<a name="requirements-kafka"/>

## Apache Kafka

The code in this repository requires Apache Kafka 0.10+ because from this point onwards Kafka includes its
[Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams) library.
See [Version Compatibility Matrix](#version-compatibility) for further details, as different branches of this
repository may have different Kafka requirements.

To add the Kafka Streams library to your application when using Confluent Platform and `maven` (see [pom.xml](pom.xml)
and
[Kafka Streams: libraries and maven artifacts](http://docs.confluent.io/current/streams/developer-guide.html#libraries-and-maven-artifacts)
for details):

```xml
<!-- pom.xml -->
<repositories>
  <repository>
    <id>confluent</id>
    <url>http://packages.confluent.io/maven/</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
    <version>0.11.0.0-cp1</version>
  </dependency>
  <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>0.11.0.0-cp1</version>
  </dependency>
</dependencies>
```

To add the Kafka Streams library to your application when using Confluent Platform and `gradle`:

```
repositories {
  maven { url "http://packages.confluent.io/maven/" }
}

dependencies {
    compile "org.apache.kafka:kafka-streams:0.11.0.0-cp1"
    compile "org.apache.kafka:kafka-clients:0.11.0.0-cp1"
}
```

To add the Kafka Streams library to your application when using Confluent Platform and `sbt` (Scala):

```scala
resolvers ++= Seq(
  "confluent-repository" at "http://packages.confluent.io/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0-cp1",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0-cp1"
)
```

<a name="requirements-confluent-platform"/>

## Confluent Platform

The code in this repository requires Confluent Platform.
See [Version Compatibility Matrix](#version-compatibility) for further details, as different branches of this
repository may have different Confluent Platform requirements.

* [Confluent Platform Quickstart](http://docs.confluent.io/current/quickstart.html) (how to download and install)
* [Confluent Platform documentation](http://docs.confluent.io/current/)

If you just run the integration tests (`mvn test`), then you do not need to install anything -- all maven artifacts
will be downloaded automatically for the build.  However, if you want to interactively test-drive the examples under
[src/main/](src/main/)
(such as [WordCountLambdaExample](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java)), then you
do need to install Confluent Platform.  See [Packaging and running the examples](#packaging-and-running) below.  Also,
each example states its exact requirements at the very top.


<a name="requirements-java"/>

## Java 8

Some code examples require Java 8, primarily because of the usage of
[lambda expressions](https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html).

IntelliJ IDEA users:

* Open _File > Project structure_
* Select "Project" on the left.
    * Set "Project SDK" to Java 1.8.
    * Set "Project language level" to "8 - Lambdas, type annotations, etc."


<a name="requirements-scala"/>

## Scala

> Scala is required only for the Scala examples in this repository.  If you are a Java developer you can safely ignore
> this section.

If you want to experiment with the Scala examples in this repository, you need a version of Scala that supports Java 8
and SAM / Java lambda (e.g. Scala 2.11 with `-Xexperimental` compiler flag, or 2.12).


<a name="packaging-and-running"/>

# Packaging and running the examples

> **Tip:** If you only want to run the integration tests (`mvn test`), then you do not need to package or install
> anything -- just run `mvn test`.  The instructions below are only needed if you want to interactively test-drive the
> examples under [src/main/](src/main/).

The first step is to install and run a Kafka cluster, which must consist of at least one Kafka broker as well as
at least one ZooKeeper instance.  Some examples may also require a running instance of Confluent schema registry.
The [Confluent Platform Quickstart](http://docs.confluent.io/current/quickstart.html) guide provides the full
details.

In a nutshell:

```shell
# Start ZooKeeper
$ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties

# In a separate terminal, start Kafka broker
$ ./bin/kafka-server-start ./etc/kafka/server.properties

# In a separate terminal, start Confluent schema registry
$ ./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties

# Again, please refer to the Confluent Platform Quickstart for details such as
# how to download Confluent Platform, how to stop the above three services, etc.
```

> Tip:  You can also run `mvn test`, which executes the included integration tests.  These tests spawn embedded Kafka
> clusters to showcase the Kafka Streams functionality end-to-end.  The benefit of the integration tests is that you
> don't need to install and run a Kafka cluster yourself.

If you want to run the examples against a Kafka cluster, you may want to create a standalone jar ("fat jar") of the
Kafka Streams examples via:

```shell
# Create a standalone jar
#
# Tip: You can also disable the test suite (e.g. to speed up the packaging
#      or to lower JVM memory usage) if needed:
#
#     $ mvn -DskipTests=true clean package
#
$ mvn clean package

# >>> Creates target/streams-examples-3.3.0-standalone.jar

```

You can now run the example applications as follows:

```shell
# Run an example application from the standalone jar.
# Here: `WordCountLambdaExample`
$ java -cp target/streams-examples-3.3.0-standalone.jar \
  io.confluent.examples.streams.WordCountLambdaExample
```

The application will try to read from the specified input topic (in the above example it is ``TextLinesTopic``), 
execute the processing logic, and then try to write back to the specified output topic (in the above example it is ``WordsWithCountsTopic``).
In order to observe the expected output stream, you will need to start a console producer to send messages into the input topic
and start a console consumer to continuously read from the output topic. More details in how to run the examples can be found
in the [java docs](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java#L29) of each example code.

If you want to turn on log4j while running your example application, you can edit the [log4j.properties](src/main/resources/log4j.properties) file 
and then execute as follows:

```shell
# Run an example application from the standalone jar.
# Here: `WordCountLambdaExample`
$ java -cp target/streams-examples-3.3.0-standalone.jar \
  -Dlog4j.configuration=file:src/main/resources/log4j.properties \
  io.confluent.examples.streams.WordCountLambdaExample
```

Keep in mind that the machine on which you run the command above must have access to the Kafka/ZK clusters you
configured in the code examples.  By default, the code examples assume the Kafka cluster is accessible via
`localhost:9092` (aka Kafka's ``bootstrap.servers`` parameter) and the ZooKeeper ensemble via `localhost:2181`.
You can override the default ``bootstrap.servers`` parameter through a command line argument.



<a name="development"/>

# Development

This project uses the standard maven lifecycle and commands such as:

```shell
$ mvn compile # This also generates Java classes from the Avro schemas
$ mvn test    # Runs unit and integration tests
```


<a name="version-compatibility"/>

# Version Compatibility Matrix

| Branch (this repo)                                                             | Apache Kafka      | Confluent Platform | Notes                                                                                 |
| -------------------------------------------------------------------------------|-------------------|--------------------|---------------------------------------------------------------------------------------|
| [master](../../../tree/master/kafka-streams)                                   | 0.11.1.0-SNAPSHOT | 3.5.0-SNAPSHOT     | You must manually build the `trunk` version of Apache Kafka and the `master` version of Confluent Platform.  See instructions above. |
| [3.3.x](../../../tree/3.3.x/kafka-streams)                                     | 0.11.0.0(-cp1)    | 3.3.0              | Works out of the box                                                                  |
| [3.2.x](../../../tree/3.2.x/kafka-streams)                                     | 0.10.2.1(-cp1)    | 3.2.1              | Works out of the box                                                                  |
| [3.1.x](../../../tree/3.1.x/kafka-streams)                                     | 0.10.1.1 [preferred], 0.10.1.0(-cp2)    | 3.1.1              | Works out of the box                                                                  |
| [kafka-0.10.0.1-cp-3.0.1](../../../tree/kafka-0.10.0.1-cp-3.0.1/kafka-streams) | 0.10.0.1(-cp1)    | 3.0.1              | Works out of the box                                                                  |
| [kafka-0.10.0.0-cp-3.0.0](../../../tree/kafka-0.10.0.0-cp-3.0.0/kafka-streams) | 0.10.0.0(-cp1)    | 3.0.0              | Works out of the box                                                                  |

The `master` branch of this repository represents active development, and may require additional steps on your side to
make it compile.  Check this README as well as [pom.xml](pom.xml) for any such information.


<a name="help"/>

# Where to find help

* Looking for documentation on Apache Kafka's Streams API?
    * We recommend to read the [Kafka Streams chapter](http://docs.confluent.io/current/streams/) in the
      [Confluent Platform documentation](http://docs.confluent.io/current/).
    * Watch our video talk
      [Introducing Kafka Streams, the new stream processing library of Apache Kafka](https://www.youtube.com/watch?v=o7zSLNiTZbA)
      ([slides](http://www.slideshare.net/MichaelNoll4/introducing-kafka-streams-the-new-stream-processing-library-of-apache-kafka-berlin-buzzwords-2016))
* Running into problems to use the demos and examples in this project?
    * First, you should check our [FAQ wiki](https://github.com/confluentinc/examples/wiki/FAQ).
    * If the FAQ doesn't help you, [create a new GitHub issue](https://github.com/confluentinc/examples/issues).
* Want to ask a question, report a bug in Kafka or its Kafka Streams API, request a new Kafka feature?
    * For general questions about Apache Kafka and Confluent Platform, please head over to the
      [Confluent mailing list](https://groups.google.com/forum/?pli=1#!forum/confluent-platform)
      or to the [Apache Kafka mailing lists](http://kafka.apache.org/contact).
    * For questions about the demos and examples in this repository:
        * Please check our [FAQ wiki](https://github.com/confluentinc/examples/wiki/FAQ) for an answer first.
        * If the FAQ doesn't help you, [create a new GitHub issue](https://github.com/confluentinc/examples/issues).
