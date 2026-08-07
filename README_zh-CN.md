# Kafka Streams 示例

<!-- hy-mt2-i18n:start -->
[English](./README.md) | **中文** | [日本語](./README_ja.md) | [Español](./README_es.md)
<!-- hy-mt2-i18n:end -->


> [!NOTE]  
> 此仓库已被 [Confluent Tutorials for Apache Kafka](https://github.com/confluentinc/tutorials) 取代。我们仍会维持其运行，但不会再对现有示例进行改进，也不会新增示例。

该项目包含了代码示例，展示了如何利用[Apache Kafka](http://kafka.apache.org/)即Kafka Streams的Streams API来实现实时应用与事件驱动型微服务。

如需更多信息，请查看
[**关于 Kafka Streams API 的最新 Confluent 文档**](http://docs.confluent.io/current/streams/)，尤其是
[**开发者指南**](https://docs.confluent.io/platform/current/streams/developer-guide/index.html)


---
目录

* [可用示例](#available-examples)
    * [示例：可运行的应用程序](#examples-apps)
    * [示例：单元测试](#examples-unit-tests)
    * [示例：集成测试](#examples-integration-tests)
    * [Docker 示例：Kafka 音乐演示应用](#examples-docker)
    * [示例：事件流处理平台](#examples-event-streaming-platform)
* [需求条件](#requirements)
    * [Apache Kafka](#requirements-kafka)
    * [Confluent Platform](#requirements-confluent-platform)
    * [使用 IntelliJ 或 Eclipse](#requirements-ide)
    * [Java](#requirements-java)
    * [Scala](#requirements-scala)
* [打包与运行示例](#packaging-and-running)
* [开发指南](#development)
* [版本兼容性矩阵](#version-compatibility)
* [获取帮助的途径](#help)

# 可用的示例


<a name="available-examples"/>

# 可用的示例

该仓库包含多个分支，可帮助您找到适用于当前所使用的 Apache Kafka 和/或 Confluent Platform 版本的正确代码示例。详情请参见下方的[版本兼容性矩阵](#version-compatibility)。

示例共有三种类型：

* **[src/main/](src/main/) 下的示例**：这些示例简短精炼。此外，您还可以通过交互方式对它们进行测试，例如在本地 Kafka 集群上运行。如果您想真正运行这些示例，则必须先安装并运行 Apache Kafka 及相关组件，具体步骤详见[打包与运行示例](#packaging-and-running)一节。每个示例的最上方都会明确说明其具体的需求及操作指南。  
* **[src/test/](src/test/) 下的示例**：这些示例用于测试[src/main/](src/main/)中的应用程序。基于TopologyTestDriver的单元测试可在无需外部系统依赖的情况下验证流处理逻辑。集成测试则使用内置的 Kafka 集群，通过标准的 Kafka 生产者客户端向其中输入数据，再利用 Kafka Streams 对数据进行处理，最后通过标准的 Kafka 消费者客户端读取并验证输出结果。这些示例也是学习如何实现自定义端到端集成测试的良好起点。  
* **可直接运行的 Docker 示例**：这些示例已经完成构建并封装在容器中。


<a name="examples-apps"/>

## 示例：可运行的应用程序

更多示例可在 [src/main/](src/main/java/io/confluent/examples/streams/) 下找到。

| 应用程序名称               | 使用的概念                                             | Java 8+ | Java 7+ | Scala |
| --------------------------- | -------------------------------------------------------- | ------- | ------- | ----- |
| WordCount                   | DSL、聚合操作、有状态处理                                 | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java) | | [Scala 示例](src/main/scala/io/confluent/examples/streams/WordCountScalaExample.scala) |
| MapFunction                 | DSL、无状态转换、`map()` 方法                              | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java) | | [Scala 示例](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala) |
| SessionWindows              | 用户事件会话化处理、用户行为分析                         | | [Java 7+ 示例](src/main/java/io/confluent/examples/streams/SessionWindowsExample.java)
| GlobalKTable                | `KStream` 与 `GlobalKTable` 之间的 `join()` 操作           | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/GlobalKTablesExample.java) | | |
| GlobalStore                 | `KStream` 与 `GlobalStore` 之间的“join”操作                 | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/GlobalStoresExample.java) | | |
| PageViewRegion              | `KStream` 与 `KTable` 之间的 `join()` 操作                 | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java) | [Java 7+ 示例](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) | |
| PageViewRegionGenericAvro   | 处理通用 Avro 格式的数据                             | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java) | [Java 7+ 示例](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) | |
| WikipediaFeedSpecificAvro   | 处理特定 Avro 格式的数据                             | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroLambdaExample.java) | [Java 7+ 示例](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroExample.java) | |
| SecureKafkaStreams          | 安全性保障、加密功能、客户端身份验证                     | | [Java 7+ 示例](src/main/java/io/confluent/examples/streams/SecureKafkaStreamsExample.java) | |
| Sum                         | DSL、有状态转换、`reduce()` 方法                          | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/SumLambdaExample.java) | | |
| WordCountInteractiveQueries | 交互式查询、REST 接口、RPC 技术                         | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExample.java) | | |
| KafkaMusic                  | 交互式查询、状态存储、REST API              | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java) | | |
| ApplicationReset            | 应用重置工具 `kafka-streams-application-reset` | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/ApplicationResetExample.java) | | |
| Microservice                | 微服务生态系统、状态存储、动态路由、连接、过滤、分支、带状态的操作 | [Java 8+ 示例](src/main/java/io/confluent/examples/streams/microservices) | | |


<a name="examples-unit-tests"/>

## 示例：单元测试

Kafka Streams 的流处理功能可以通过 `org.apache.kafka:kafka-streams-test-utils` 库中的 `TopologyTestDriver` 进行**单元测试**。该测试驱动工具允许你向处理拓扑中输入示例数据，并验证其输出结果。

请参阅[测试流代码](https://docs.confluent.io/current/streams/developer-guide/test-streams.html)中的文档。


<a name="examples-integration-tests"/>

## 示例：集成测试

我们还提供了若干**集成测试**，用于演示端到端的数据处理管道。在这些测试中，我们会启动嵌入式的 Kafka 集群以及 [Confluent Schema Registry](https://github.com/confluentinc/schema-registry)，通过标准的 Kafka 生产者客户端向其中输入数据，利用 Kafka Streams 对数据进行处理，最后再通过标准的 Kafka 消费者客户端读取并验证输出结果。

更多示例可在 [src/test/](src/test/java/io/confluent/examples/streams/) 下找到。

提示：运行 `mvn test` 即可启动测试。

| 集成测试名称                       | 使用的概念                                 | Java 8+ | Java 7+ | Scala |
| ----------------------------------- | ------------------------------------------- | ------- | ------- | ----- |
| WordCount                           | DSL、聚合、有状态处理                         | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java) | | [Scala 示例](src/test/scala/io/confluent/examples/streams/WordCountScalaIntegrationTest.scala) |
| WordCountInteractiveQueries         | 交互式查询、REST、RPC                      | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExampleTest.java) | |
| Aggregate                           | DSL、`groupBy()`、`aggregate()`                 | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/AggregateTest.java) | | [Scala 示例](src/test/scala/io/confluent/examples/streams/AggregateScalaTest.scala) |
| CustomStreamTableJoin               | DSL、Processor API、转换器                   | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/CustomStreamTableJoinIntegrationTest.java) | | |
| EventDeduplication                  | DSL、Processor API、转换器                   | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java) | | |
| GlobalKTable                        | DSL、全局状态                               | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/GlobalKTablesExampleTest.java) | |
| GlobalStore                         | DSL、全局状态、转换器                       | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/GlobalStoresExampleTest.java) | |
| HandlingCorruptedInputRecords       | DSL、`flatMap()`                            | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java) | | |
| KafkaMusic (Interactive Queries)    | 交互式查询、状态存储、REST API                | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExampleTest.java) | |
| MapFunction                         | DSL、无状态转换、`map()`                     | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/MapFunctionLambdaIntegrationTest.java) | | |
| MixAndMatch DSL + Processor API     | 结合使用 DSL 和 Processor API                | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/MixAndMatchLambdaIntegrationTest.java) | | |
| PassThrough                         | DSL、`stream()`、`to()`                     | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/PassThroughIntegrationTest.java) | |
| PoisonPill                          | DSL、`flatMap()`                            | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java) | | |
| ProbabilisticCounting\*\*\*         | DSL、Processor API、自定义状态存储           | | | [Scala 示例](src/test/scala/io/confluent/examples/streams/ProbabilisticCountingScalaIntegrationTest.scala) |
| Reduce (Concatenate)                | DSL、`groupByKey()`、`reduce()`             | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/ReduceTest.java) | | [Scala 示例](src/test/scala/io/confluent/examples/streams/ReduceScalaTest.scala) |
| SessionWindows                      | DSL、窗口聚合、会话化处理                 | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/SessionWindowsExampleTest.java) | |
| StatesStoresDSL                     | DSL、Processor API、Transformers            | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/StateStoresInTheDSLIntegrationTest.java) | | |
| StreamToStreamJoin                  | DSL、KStream 之间的 `join()` 操作           | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/StreamToStreamJoinIntegrationTest.java) | |
| StreamToTableJoin                   | DSL、KStream 与 KTable 之间的 `join()` 操作   | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/StreamToTableJoinIntegrationTest.java) | [Scala 示例](src/test/scala/io/confluent/examples/streams/StreamToTableJoinScalaIntegrationTest.scala) |
| Sum                                 | DSL、聚合操作、带状态处理、`reduce()`      | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/SumLambdaIntegrationTest.java) | | |
| TableToTableJoin                    | DSL、KTable 之间的 `join()` 操作           | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/TableToTableJoinIntegrationTest.java) | |
| UserCountsPerRegion                 | DSL、聚合操作、带状态处理、`count()`       | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/UserCountsPerRegionLambdaIntegrationTest.java) | | |
| ValidateStateWithInteractiveQueries | 用于验证状态的交互式查询               | | [Java 8+ 示例](src/test/java/io/confluent/examples/streams/ValidateStateWithInteractiveQueriesLambdaIntegrationTest.java) | | |
| GenericAvro                         | 处理通用 Avro 格式的数据                 | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/GenericAvroIntegrationTest.java) |  [Scala 示例](src/test/scala/io/confluent/examples/streams/GenericAvroScalaIntegrationTest.scala) |
| SpecificAvro                        | 处理特定 Avro 格式的数据           | | [Java 7+ 示例](src/test/java/io/confluent/examples/streams/SpecificAvroIntegrationTest.java) | [Scala 示例](src/test/scala/io/confluent/examples/streams/SpecificAvroScalaIntegrationTest.scala) |

***演示了如何通过实现一个由[Count-Min Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch)数据结构支持的定制状态存储（[CMSStore](src/main/scala/io/confluent/examples/streams/algebird/CMSStore.scala)）（基于[Twitter Algebird](https://github.com/twitter/algebird)的CMS实现），来以概率方式统计输入流中的元素数量***


<a name="examples-docker"/>

# Docker 示例：Kafka Music 演示应用

该容器化示例会启动：

* 基于 Kafka Streams API 的 Confluent Kafka Music 演示应用，该应用利用了[交互式查询](http://docs.confluent.io/current/streams/developer-guide.html)功能  
* 一个包含单节点 ZooKeeper 集群的单节点 Apache Kafka 集群  
* 一个[Confluent Schema Registry](https://github.com/confluentinc/schema-registry) 实例

Kafka Music应用程序展示了如何构建一个简单的音乐排行榜应用，该应用能够实时持续计算出各类音乐流派的最新Top 5歌曲等排行榜数据。它通过Kafka的[交互式查询](http://docs.confluent.io/current/streams/developer-guide.html#interactive-queries)功能，并借助REST API，将最新的处理结果——即最新的排行榜——呈现出来。该应用的输入数据为Avro格式，因此需要使用Confluent Schema Registry，而这些数据来自两个来源：播放事件流（例如“歌曲X已被播放”）以及歌曲元数据流（例如“歌曲X由艺术家Y创作”）。

详细文档可查阅
https://docs.confluent.io/current/streams/kafka-streams-examples/docs/index.html。


<a name="event-streaming-platform"/>

# 示例：事件流处理平台

如需更多展示在事件流处理平台中应用 Kafka Streams 的示例，请参阅 [examples GitHub 仓库](https://github.com/confluentinc/examples)。


# 需求条件

# 要求条件

<a name="requirements-kafka"/>

## Apache Kafka

该仓库中的代码需要 Apache Kafka 0.10 及更高版本，因为从该版本开始 Kafka 已内置了其 [Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams) 库。更多详细信息请参阅[版本兼容性矩阵](#version-compatibility)，因为该仓库的不同分支可能对 Kafka 的版本要求各不相同。

> **对于 `master` 分支：** 要构建开发版本，通常需要最新版本的 Apache Kafka `trunk` 版本（详情请参见 [pom.xml](pom.xml) 中的 `kafka.version`）。以下指令将用于构建并本地安装最新版本的 `trunk` Kafka：
>
> ```shell
> $ git clone git@github.com:apache/kafka.git
> $ cd kafka
> $ git checkout trunk
>
> # 接下来在本地构建并安装 Kafka
> $./gradlew clean &&./gradlewAll install
> ```


<a name="requirements-confluent-platform"/>

## Confluent Platform

该仓库中的代码需要 [Confluent Schema Registry](https://github.com/confluentinc/schema-registry)。由于该仓库的不同分支对 Confluent Platform 的要求各不相同，详情请参阅[版本兼容性矩阵](#version-compatibility)。

* [Confluent Platform 快速入门](http://docs.confluent.io/current/quickstart.html)（下载与安装方法）
* [Confluent Platform 文档](http://docs.confluent.io/current/)

> **对于 `master` 分支：** 要构建开发版本，通常需要 Confluent Platform 的 Schema Registry 的最新 `master` 版本（参见 [pom.xml](pom.xml) 中的 `confluent.version`，该值由上游的 [Confluent Common](https://github.com/confluentinc/common) 项目设定）。以下指令将构建并本地安装最新的 `master` 版 Schema Registry，其中还会包含其依赖项，如 [Confluent Common](https://github.com/confluentinc/common) 和 [Confluent Rest Utils](https://github.com/confluentinc/rest-utils) 的构建过程。详情请参阅 [Schema Registry README](https://github.com/confluentinc/schema-registry)。
>
> ```shell
> $ git clone https://github.com/confluentinc/common.git
> $ cd common
> $ git checkout master
>
> # 在本地构建并安装 common
> $ mvn -DskipTests=true clean install
>
> $ git clone https://github.com/confluentinc/rest-utils.git
> $ cd rest-utils
> $ git checkout master
>
> # 在本地构建并安装 rest-utils
> $ mvn -DskipTests=true clean install
>
> $ git clone https://github.com/confluentinc/schema-registry.git
> $ cd schema-registry
> $ git checkout master
>
> # 现在在本地构建并安装 schema-registry
> $ mvn -DskipTests=true clean install
> ```

此外，每个示例在最上方都会明确说明其具体要求。


<a name="requirements-ide"/>

## 使用 IntelliJ 或 Eclipse

如果您使用 IDE 并导入该项目，可能会出现“缺少导入/类未找到”的错误。
某些 Avro 类是由模式文件生成的，而 IDE 有时不会自动生成这些类。
要解决此错误，请手动运行：

```shell
$ mvn -DskipTests=true compile
```

如果使用的是 Eclipse，也可以右键点击 `pom.xml` 文件，然后选择“运行作为 > Maven generate-sources”。


## Java版本要求
> 仅本仓库中的Java示例需要使用Java。如果您是Java开发者，可以安全地忽略此部分内容。

## Java 17+

IntelliJ IDEA 用户：

* 打开 _File > Project structure_  
* 在左侧选择“Project”。  
    * 将“Project SDK”设置为 Java 17。  
    * 将“Project language level”设置为“17 - Sealed types, always-strict floating-point semantics”。


<a name="requirements-scala"/>

## Scala

只有要使用本仓库中的 Scala 示例时才需要 Scala。如果您是 Java 开发者，完全可以忽略此部分内容。

如果您想试用本仓库中的 Scala 示例，需要使用支持 Java 17 的 Scala 版本。

<a name="packaging-and-running"># 打包与运行应用程序示例</a>

# 应用示例的打包与运行

只有当您想要在 [src/main/](src/main/) 下对[应用程序示例](#examples-apps)进行交互式测试时，才需要遵循本节中的说明。

> **提示：** 如果您只想运行集成测试（`mvn test`），则无需进行任何打包或安装操作——直接运行 `mvn test` 即可。这些测试会启动内置的 Kafka 集群。

第一步是安装并运行一个Kafka集群，该集群至少需要包含一个Kafka代理以及一个ZooKeeper实例。某些示例可能还要求有正在运行的Confluent模式注册表实例。[Confluent Platform快速入门](http://docs.confluent.io/current/quickstart.html)指南提供了详细说明。

简而言之：

```shell
# 请确保已按照上述快速入门指南下载并安装了 Confluent Platform。

# 生成集群 UUID
$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# 格式化日志目录
$ bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/kraft/reconfig-server.properties

# 启动 Kafka 代理
$./bin/kafka-server-start./etc/kafka/server.properties

# 在另一个终端中启动 Confluent Schema Registry
$./bin/schema-registry-start./etc/schema-registry/schema-registry.properties

# 更多详细信息，例如如何下载 Confluent Platform、如何停止上述三种服务等，请参考 Confluent Platform 快速入门指南。

下一步是为[应用程序示例](#examples-apps)创建一个独立可执行的 jar 文件（“胖 jar”）：

```shell
# 创建独立 JAR 文件（“胖 JAR”）
$ mvn clean package

# >>> 生成 target/kafka-streams-examples-8.4.0-0-standalone.jar 文件
```

提示：如有需要，您可以在打包过程中禁用测试套件，例如为了加快打包速度或降低JVM内存占用：
>
> ```shell
> $ mvn -DskipTests=true clean package
> ```

现在可以按照以下方式运行这些应用程序示例：

```shell
# 运行独立 JAR 中的示例应用程序，此处为 `WordCountLambdaExample`
$ java -cp target/kafka-streams-examples-8.4.0-0-standalone.jar \
  io.confluent.examples.streams.WordCountLambdaExample
```

该应用程序会尝试从指定的输入主题（在上面的示例中为“streams-plaintext-input”）读取数据，执行处理逻辑，然后再尝试写入到指定的输出主题（在上面的示例中为“streams-wordcount-output”）。为了查看预期的输出流，你需要启动一个控制台生产者向输入主题发送消息，同时启动一个控制台消费者持续从输出主题读取数据。关于如何运行这些示例的更多详细信息，可查阅每个示例代码的[Java文档](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java#L31)。

如果您希望在运行示例应用程序时启用 log4j2，可以编辑 [log4j2.yaml](src/main/resources/log4j2.yaml) 文件，然后按如下方式执行：

```shell
# 运行独立 JAR 包中的示例应用程序，此处为 `WordCountLambdaExample`
$ java -cp target/kafka-streams-examples-8.4.0-0-standalone.jar \
  -Dlog4j2.configurationFile=src/main/resources/log4j2.yaml \
  io.confluent.examples.streams.WordCountLambdaExample
```

请注意，运行上述命令的机器必须能够访问你在代码示例中配置的 Kafka/ZooKeeper 集群。默认情况下，这些代码示例假定可以通过 `localhost:9092`（即 Kafka 的 `bootstrap.servers` 参数）访问 Kafka 集群，通过 `localhost:2181` 访问 ZooKeeper 集群。你也可以通过命令行参数来覆盖默认的 `bootstrap.servers` 参数。


# 开发</a>

# 开发指南

该项目采用标准的 Maven 生命周期及以下命令：

```shell
$ mvn compile # 此操作还会根据 Avro 模式生成 Java 类
$ mvn test    # 运行单元测试和集成测试
$ mvn package # 将应用程序示例打包为独立的 jar 文件
```


<a name="version-compatibility"/>

# 版本兼容性矩阵

| 本仓库的分支                        | Confluent Platform | Apache Kafka      |
| ----------------------------------------|--------------------|-------------------|
| [master](../../../tree/master/)\*       | 8.0.0-SNAPSHOT     | 4.0.0-SNAPSHOT    |
| [7.9.x](../../../tree/7.9.x/)           | 7.9.0-SNAPSHOT     | 3.9.0             |
| [7.8.0-post](../../../tree/7.8.0-post/) | 7.8.0              | 3.8.0             |
|...                                     |                    |                   |
| [7.1.0-post](../../../tree/7.1.0-post/) | 7.1.0              | 3.1.0             |

7.1.0 之前的旧版本已[不再支持](https://docs.confluent.io/platform/current/installation/versions-interoperability.html)。

\*您需要手动构建Apache Kafka的`4.0`版本以及Confluent Platform的`8.0.0`版本。请参阅上述说明。

该仓库的 `master` 分支代表正在进行的开发版本，可能需要您采取额外步骤才能使其成功编译。请查看本 README 以及 [pom.xml](pom.xml) 中的相关说明。


<a name="help"/>

# 在何处获取帮助

* 需要查找关于 Apache Kafka Streams API 的文档？
    * 我们建议阅读 [Confluent Platform 文档](https://docs.confluent.io/current/) 中的
      [Kafka Streams 章节](https://docs.confluent.io/current/streams/)。
    * 观看我们的演讲
      [利用 Apache Kafka 重新思考流处理](https://www.youtube.com/watch?v=ACwnrnVJXuE)
* 在使用本项目中的演示和示例时遇到问题？
    * 首先，您应该查看我们的 [FAQ 维基页面](https://github.com/confluentinc/kafka-streams-examples/wiki/FAQ)以寻找答案。
    * 如果 FAQ 无法帮到您，请[创建一个新的 GitHub issue](https://github.com/confluentinc/kafka-streams-examples/issues)。
* 想提问、报告 Kafka 或其 Kafka Streams API 的错误，或请求新增 Kafka 功能？
    * 对于关于 Apache Kafka 和 Confluent Platform 的一般性问题，请前往
      [Confluent 邮件列表](https://groups.google.com/forum/?pli=1#!forum/confluent-platform)
      或 [Apache Kafka 邮件列表](http://kafka.apache.org/contact)。

# 许可证

使用此镜像需遵守其中所含软件的许可条款。如需更多信息，请参阅 Confluent 的 Docker 镜像文档[参考资料](https://docs.confluent.io/platform/current/installation/docker/image-reference.html)。用于扩展和构建自定义 Docker 镜像的软件则遵循 Apache 2.0 许可协议。
