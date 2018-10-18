# Kafka Streams Examples

This project contains code examples that demonstrate how to implement real-time applications and event-driven
microservices using the Streams API of [Apache Kafka](http://kafka.apache.org/) aka Kafka Streams.

For more information take a look at the
[**latest Confluent documentation on the Kafka Streams API**](http://docs.confluent.io/current/streams/), notably the
[**Developer Guide**](http://docs.confluent.io/current/streams/developer-guide.html).


---
Table of Contents

* [Available examples](#available-examples)
    * [Java](#examples)
    * [Scala](#examples)
* [Requirements](#requirements)
    * [Apache Kafka](#requirements-kafka)
    * [Confluent Platform](#requirements-confluent-platform)
    * [Using IntelliJ or Eclipse](#requirements-ide)
    * [Java](#requirements-java)
    * [Scala](#requirements-scala)
* [Packaging and running the examples](#packaging-and-running)
* [Development](#development)
* [Version Compatibility Matrix](#version-compatibility)
* [Docker](#docker)
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

## Examples

<a name="examples"/>

| Name | Concepts used | Java 8+ | Java 7+ | Scala |
| --- | --- | --- | --- | --- |
| WordCount | DSL | [Java 8+ example](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java)
| MapFunction | DSL, stateless transformations, map() | [Java 8+ example](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java) | |   [Scala Example](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala) |
| SessionWindows | Sessionization of user events, user behavior analysis  | | [Java 7+ example](src/main/java/io/confluent/examples/streams/SessionWindowsExample.java)
| Sum | DSL, stateful transformations, `reduce` | [Java 8+ example](src/main/java/io/confluent/examples/streams/SumLambdaExample.java)
| PageViewRegion | `join` between `KStream` and `KTable` | [Java 8+ example](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java) | [Java 7+ example](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) | |
| PageViewRegionGenericAvro | Generic Avro | [Java 8+ example](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java) | [Java 7+ example](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java)
| WikipediaFeedSpecificAvro | Specific Avro | [Java 8+ example](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroLambdaExample.java) | [Java 7+ example](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroExample.java)
| SecureKafkaStreams | Secure, encryption, client authentication | | [Java 7+ example](src/main/java/io/confluent/examples/streams/SecureKafkaStreamsExample.java)
| StatesStoresDSL | State Stores, DSL | [Java 8+ example](src/test/java/io/confluent/examples/streams/StateStoresInTheDSLIntegrationTest.java)
| WordCountInteractiveQueries | Interactive Queries, REST, RPC | [Java 8+ example](src/main/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExample.java)
| KafkaMusic | Interactive Queries, State Stores, REST API | [Java 8+ example](src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java)
| PoisonPill | Corrupt input records | [Java 8+ example](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java)
| MixAndMatch DSL+Processor | DSL, Processor API, `KStream#transform()`, `KStream#process()`, custom `Transformer` and `Processor` implementations | [Java 8+ example](src/test/java/io/confluent/examples/streams/MixAndMatchLambdaIntegrationTest.java)
| ApplicationReset | Application Reset Tool `bin/kafka-streams-application-reset` | [Java 8+ example](src/main/java/io/confluent/examples/streams/ApplicationResetExample.java)
| GlobalKTable |join between `KStream` and `GlobalKTable`| [Java 8+ example](src/main/java/io/confluent/examples/streams/GlobalKTablesExample.java)
| Microservice | Microservice ecosystem | [Java 8+ example](src/main/java/io/confluent/examples/streams/microservices)

## Additional examples

Additional examples may be found [here](src/main/java/io/confluent/examples/streams/).

## Integration Tests

We also provide several **integration tests**, which demonstrate end-to-end data pipelines.  Here, we spawn embedded Kafka
clusters and the [Confluent Schema Registry](https://github.com/confluentinc/schema-registry), feed input data to them
(using the standard Kafka producer client), process the data using Kafka Streams, and finally read and verify the output
results (using the standard Kafka consumer client).

> Tip: Run `mvn test` to launch the integration tests.

| Integration Test Name  | Java 8+ | Java 7+ | Scala |
| --- | --- | --- | --- |
| WordCount | [Java 8+ Integration Test](src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java) | | [Scala Integration Test](src/test/scala/io/confluent/examples/streams/WordCountScalaIntegrationTest.scala)
| WordCountInteractiveQueries | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExampleTest.java)
| EventDeduplication | [Java 8+ Integration Test](src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java)
| GlobalKTable | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/GlobalKTablesExampleTest.java)
| HandlingCorruptedInputRecords | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java)
| KafkaMusic | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExampleTest.java)
| MapFunction | [Java 8+ Integration Test](src/test/java/io/confluent/examples/streams/MapFunctionLambdaIntegrationTest.java)
| MixAndMatch DSL+Processor | [Java 8+ Integration Test](src/test/java/io/confluent/examples/streams/MixAndMatchLambdaIntegrationTest.java)
| PassThrough | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/PassThroughIntegrationTest.java)
| SessionWindows | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/SessionWindowsExampleTest.java)
| Sum | [Java 8+ Integration Test](src/test/java/io/confluent/examples/streams/SumLambdaIntegrationTest.java)
| StreamToStreamJoin | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/StreamToStreamJoinIntegrationTest.java)
| StreamToTableJoin | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/StreamToTableJoinIntegrationTest.java) |  [Scala Integration Test](src/test/scala/io/confluent/examples/streams/StreamToTableJoinScalaIntegrationTest.scala)
| TableToTableJoin | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/TableToTableJoinIntegrationTest.java)
| UserCountsPerRegion | [Java 8+ Integration Test](src/test/java/io/confluent/examples/streams/UserCountsPerRegionLambdaIntegrationTest.java)
| GenericAvro | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/GenericAvroIntegrationTest.java) |  [Scala Integration Test](src/test/scala/io/confluent/examples/streams/GenericAvroScalaIntegrationTest.scala)
| SpecificAvro | | [Java 7+ Integration Test](src/test/java/io/confluent/examples/streams/SpecificAvroIntegrationTest.java) | [Scala Integration Test](src/test/scala/io/confluent/examples/streams/SpecificAvroScalaIntegrationTest.scala)
| ValidateStateWithInteractiveQueries | [Java 8+ Integration Test](src/test/java/io/confluent/examples/streams/ValidateStateWithInteractiveQueriesLambdaIntegrationTest.java)
| ProbabilisticCounting*** | | | [Scala Integration Test](src/test/scala/io/confluent/examples/streams/ProbabilisticCountingScalaIntegrationTest.scala)


***demonstrates how to probabilistically count items in an input stream by implementing a custom state store
  ([CMSStore](src/main/scala/io/confluent/examples/streams/algebird/CMSStore.scala)) that is backed by a
  [Count-Min Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) data structure (with the CMS implementation
  of [Twitter Algebird](https://github.com/twitter/algebird)

<a name="requirements"/>

# Requirements

<a name="requirements-kafka"/>

## Apache Kafka

The code in this repository requires Apache Kafka 0.10+ because from this point onwards Kafka includes its
[Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams) library.
See [Version Compatibility Matrix](#version-compatibility) for further details, as different branches of this
repository may have different Kafka requirements.

> **For the `master` branch:** To build a development version, you typically need the latest `trunk` version of Apache Kafka
> (cf. `kafka.version` in [pom.xml](pom.xml) for details).  The following instructions will build and locally install
> the latest `trunk` Kafka version:
>
> ```shell
> $ git clone git@github.com:apache/kafka.git
> $ cd kafka
> $ git checkout trunk
>
> # Bootstrap gradle wrapper
> $ gradle
>
> # Now build and install Kafka locally
> $ ./gradlew clean installAll
> ```

<a name="requirements-confluent-platform"/>

## Confluent Platform

The code in this repository requires [Confluent Schema Registry](https://github.com/confluentinc/schema-registry).
And to build Confluent Schema Registry in its development version, further dependencies of Confluent Platform are needed (e.g.
[Confluent Common](https://github.com/confluentinc/common) and
[Confluent Rest Utils](https://github.com/confluentinc/rest-utils), 
please read its own [README](https://github.com/confluentinc/schema-registry) file for details).
See [Version Compatibility Matrix](#version-compatibility) for further details, as different branches of this
repository may have different Confluent Platform requirements.

* [Confluent Platform Quickstart](http://docs.confluent.io/current/quickstart.html) (how to download and install)
* [Confluent Platform documentation](http://docs.confluent.io/current/)

> **For the `master` branch:** To build a development version, you typically need the latest `master` version of Confluent Platform's
> Schema Registry (cf. `confluent.version` in [pom.xml](pom.xml) for details). The following instructions will build and locally install
> the latest `master` Schema Registry version:
>
> ```shell
> $ git clone https://github.com/confluentinc/common.git
> $ cd common
> $ git checkout master
>
> # Build and install common locally
> $ mvn -DskipTests=true clean install
>
> $ git clone https://github.com/confluentinc/rest-utils.git
> $ cd rest-utils
> $ git checkout master
>
> # Build and install rest-utils locally
> $ mvn -DskipTests=true clean install
>
> $ git clone https://github.com/confluentinc/schema-registry.git
> $ cd schema-registry
> $ git checkout master
>
> # Now build and install schema-registry locally
> $ mvn -DskipTests=true clean install
> ```

Also, each example states its exact requirements at the very top.


<a name="requirements-ide"/>

## Using IntelliJ or Eclipse

If you are using an IDE and import the project you might end up with a "missing import / class not found" error.
Some Avro classes are generated from schema files and the IDE does not generete those classes automatically.
You can run `mvn -Dskip.tests=true compile` manually (c.f. the steps above) to resolve the error.
If you are using Eclipse, you can also right-click on `pom.xml` file and choose `Run As` -> `Maven generate-sources`.


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

# >>> Creates target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar

```

You can now run the example applications as follows:

```shell
# Run an example application from the standalone jar.
# Here: `WordCountLambdaExample`
$ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar \
  io.confluent.examples.streams.WordCountLambdaExample
```

The application will try to read from the specified input topic (in the above example it is ``TextLinesTopic``),
execute the processing logic, and then try to write back to the specified output topic (in the above example it is ``WordsWithCountsTopic``).
In order to observe the expected output stream, you will need to start a console producer to send messages into the input topic
and start a console consumer to continuously read from the output topic. More details in how to run the examples can be found
in the [java docs](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java#L29) of each example code.

If you want to turn on log4j while running your example application, you can edit the
[log4j.properties](src/main/resources/log4j.properties) file and then execute as follows:

```shell
# Run an example application from the standalone jar.
# Here: `WordCountLambdaExample`
$ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar \
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

| Branch (this repo)                      | Apache Kafka      | Confluent Platform | Notes                                                                                                                                |
| ----------------------------------------|-------------------|--------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| [master](../../../tree/master/)         | 1.0.0-SNAPSHOT    | 4.0.0-SNAPSHOT     | You must manually build the `trunk` version of Apache Kafka and the `master` version of Confluent Platform.  See instructions above. |
| [3.3.x](../../../tree/3.3.x/)           | 0.11.0.1-SNAPSHOT | 3.3.1-SNAPSHOT     | You must manually build the `0.11.0` version of Apache Kafka and the `3.3.x` version of Confluent Platform.  See instructions above. |
| [3.3.0-post](../../../tree/3.3.0-post/) | 0.11.0.0(-cp1)    | 3.3.0              | Works out of the box                                                                                                                 |

The `master` branch of this repository represents active development, and may require additional steps on your side to
make it compile.  Check this README as well as [pom.xml](pom.xml) for any such information.


<a name="docker"/>

# Docker Examples

This example launches:

* Confluent's Kafka Music demo application for the Kafka Streams API.  This application demonstrates how to build of a simple music charts application.  It uses Kafka's
  [Interactive Queries](http://docs.confluent.io/current/streams/developer-guide.html#interactive-queries) feature to
  expose its latest processing results (e.g. latest Top 5 songs) via a REST API.  Its input data is in Avro format,
  hence the use of Confluent Schema Registry (see below).
* a single-node Apache Kafka cluster with a single-node ZooKeeper ensemble
* a [Confluent Schema Registry](https://github.com/confluentinc/schema-registry) instance

The Kafka Music application demonstrates how to build of a simple music charts application that continuously computes,
in real-time, the latest charts such as Top 5 songs per music genre.  It exposes its latest processing results -- the
latest charts -- via Kafka’s Interactive Queries feature and a REST API.  The application's input data is in Avro format
and comes from two sources: a stream of play events (think: "song X was played") and a stream of song metadata ("song X
was written by artist Y").

More specifically, we will run the following services:

* Confluent's Kafka Music demo application
* a single-node Kafka cluster with a single-node ZooKeeper ensemble
* Confluent Schema Registry

You can find detailed documentation at
http://docs.confluent.io/current/streams/kafka-streams-examples/docs/index.html


<a name="help"/>

# Where to find help

* Looking for documentation on Apache Kafka's Streams API?
    * We recommend to read the [Kafka Streams chapter](http://docs.confluent.io/current/streams/) in the
      [Confluent Platform documentation](http://docs.confluent.io/current/).
    * Watch our talk
      [Rethinking Stream Processing with Apache Kafka](https://www.youtube.com/watch?v=ACwnrnVJXuE)
* Running into problems to use the demos and examples in this project?
    * First, you should check our [FAQ wiki](https://github.com/confluentinc/kafka-streams-examples/wiki/FAQ).
    * If the FAQ doesn't help you, [create a new GitHub issue](https://github.com/confluentinc/kafka-streams-examples/issues).
* Want to ask a question, report a bug in Kafka or its Kafka Streams API, request a new Kafka feature?
    * For general questions about Apache Kafka and Confluent Platform, please head over to the
      [Confluent mailing list](https://groups.google.com/forum/?pli=1#!forum/confluent-platform)
      or to the [Apache Kafka mailing lists](http://kafka.apache.org/contact).
    * For questions about the demos and examples in this repository:
        * Please check our [FAQ wiki](https://github.com/confluentinc/kafka-streams-examples/wiki/FAQ) for an answer first.
        * If the FAQ doesn't help you, [create a new GitHub issue](https://github.com/confluentinc/kafka-streams-examples/issues).
