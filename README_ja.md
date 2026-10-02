# Kafka Streamsのサンプルコード

<!-- hy-mt2-i18n:start -->
[English](./README.md) | [中文](./README_zh-CN.md) | **日本語** | [Español](./README_es.md)
<!-- hy-mt2-i18n:end -->


> [!NOTE]  
> このリポジトリは [Confluent Tutorials for Apache Kafka](https://github.com/confluentinc/tutorials) に置き換えられました。引き続き運用は続けていますが、既存のサンプルの改良や新たなサンプルの追加は行っていません。

このプロジェクトには、[Apache Kafka](http://kafka.apache.org/)、別名Kafka StreamsのStreams APIを使用してリアルタイムアプリケーションやイベント駆動型マイクロサービスを実装する方法を示すコード例が含まれています。

詳細については、
[**Kafka Streams APIに関する最新のConfluentドキュメント**](http://docs.confluent.io/current/streams/)をご覧ください。特に
[**開発者ガイド**](https://docs.confluent.io/platform/current/streams/developer-guide/index.html)が参考になります。


---
目次

* [利用可能な例](#available-examples)
    * [例：実行可能なアプリケーション](#examples-apps)
    * [例：単体テスト](#examples-unit-tests)
    * [例：統合テスト](#examples-integration-tests)
    * [Docker例：Kafka Musicデモアプリケーション](#examples-docker)
    * [例：イベントストリーミングプラットフォーム](#examples-event-streaming-platform)
* [要件](#requirements)
    * [Apache Kafka](#requirements-kafka)
    * [Confluent Platform](#requirements-confluent-platform)
    * [IntelliJまたはEclipseの使用](#requirements-ide)
    * [Java](#requirements-java)
    * [Scala](#requirements-scala)
* [例のパッケージ化と実行方法](#packaging-and-running)
* [開発](#development)
* [バージョン互換性マトリックス](#version-compatibility)
* [ヘルプの入手先](#help)

---


<a name="available-examples"/>

# 利用可能なサンプル

このリポジトリには、ご使用のApache Kafkaおよび/またはConfluent Platformのバージョンに合った正しいコード例を見つけやすくするための複数のブランチがあります。詳細については、下記の[バージョン互換性マトリックス](#version-compatibility)をご覧ください。

例には3種類あります：

# 利用可能なサンプル
このリポジトリには、使用しているApache Kafkaおよび/またはConfluent Platformのバージョンに合った正しいコードサンプルを見つけやすくするための複数のブランチがあります。詳細は下記の[バージョン互換性マトリックス](#version-compatibility)をご覧ください。

サンプルには3種類あります：


<a name="examples-apps"/>

## 例：実行可能なアプリケーション

その他の例は、[src/main/](src/main/java/io/confluent/examples/streams/)にあります。

| アプリケーション名            | 使用されているコンセプト                                      | Java 8+ | Java 7+ | Scala |
| --------------------------- | -------------------------------------------------------- | ------- | ------- | ----- |
| WordCount                   | DSL、集約処理、ステートフル処理                              | [Java 8+ の例](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java) | | [Scala の例](src/main/scala/io/confluent/examples/streams/WordCountScalaExample.scala) |
| MapFunction                 | DSL、ステートレス変換、`map()`                                | [Java 8+ の例](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java) | | [Scala の例](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala) |
| SessionWindows              | ユーザーイベントのセッショニング、ユーザー行動分析            | | [Java 7+ の例](src/main/java/io/confluent/examples/streams/SessionWindowsExample.java)
| GlobalKTable                | `KStream` と `GlobalKTable` 間の `join()`                     | [Java 8+ の例](src/main/java/io/confluent/examples/streams/GlobalKTablesExample.java) | | |
| GlobalStore                 | `KStream` と `GlobalStore` 間の「join」                      | [Java 8+ の例](src/main/java/io/confluent/examples/streams/GlobalStoresExample.java) | | |
| PageViewRegion              | `KStream` と `KTable` 間の `join()`                          | [Java 8+ の例](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java) | [Java 7+ の例](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) | |
| PageViewRegionGenericAvro   | Generic Avro 形式のデータの処理                            | [Java 8+ の例](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java) | [Java 7+ の例](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) | |
| WikipediaFeedSpecificAvro   | Specific Avro 形式のデータの処理                            | [Java 8+ の例](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroLambdaExample.java) | [Java 7+ の例](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroExample.java) | |
| SecureKafkaStreams          | セキュリティ、暗号化、クライアント認証                        | | [Java 7+ の例](src/main/java/io/confluent/examples/streams/SecureKafkaStreamsExample.java) | |
| Sum                         | DSL、ステートフル変換、`reduce()`                             | [Java 8+ の例](src/main/java/io/confluent/examples/streams/SumLambdaExample.java) | | |
| WordCountInteractiveQueries | インタラクティブクエリ、REST、RPC                           | [Java 8+ の例](src/main/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExample.java) | | |
| KafkaMusic                  | インタラクティブクエリ、ステートストア、REST API              | [Java 8+ の例](src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java) | | |
| ApplicationReset            | アプリケーションリセットツール `kafka-streams-application-reset` | [Java 8+ の例](src/main/java/io/confluent/examples/streams/ApplicationResetExample.java) | | |
| Microservice                | マイクロサービスエコシステム、ステートストア、動的ルーティング、ジョイン、フィルタリング、分岐、ステートフル操作 | [Java 8+ の例](src/main/java/io/confluent/examples/streams/microservices) | | |


<a name="examples-unit-tests"/>

## 例：単体テスト

Kafka Streamsのストリーム処理は、`org.apache.kafka:kafka-streams-test-utils`アーティファクトに含まれる`TopologyTestDriver`を使用して**単体テスト**を行うことができます。このテストドライバーを使えば、処理トポロジーにサンプル入力を書き込み、その出力を検証することが可能です。

[Testing Streams Code](https://docs.confluent.io/current/streams/developer-guide/test-streams.html) のドキュメントをご覧ください。


<a name="examples-integration-tests"/>

## 例：統合テスト

また、エンドツーエンドのデータパイプラインを示すいくつかの**統合テスト**も用意しています。ここでは、組み込み型のKafkaクラスターおよび[Confluent Schema Registry](https://github.com/confluentinc/schema-registry)を起動し、標準的なKafkaプロデューサークライアントを使って入力データを送信し、Kafka Streamsを利用してデータを処理した後、標準的なKafkaコンシューマークライアントを使って出力結果を読み取り検証します。

その他の例については、[src/test/](src/test/java/io/confluent/examples/streams/) で確認できます。

ヒント：テストを実行するには `mvn test` を実行してください。

| 統合テスト名 | 使用されるコンセプト | Java 8+ | Java 7+ | Scala |
| -------------- | ------------------ | ------- | ------- | ----- |
| WordCount       | DSL、集計、ステートフル処理 | [Java 8+ の例](src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java) | | [Scala の例](src/test/scala/io/confluent/examples/streams/WordCountScalaIntegrationTest.scala) |
| WordCountInteractiveQueries | インタラクティブクエリ、REST、RPC | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExampleTest.java) | |
| Aggregate      | DSL、`groupBy()`、`aggregate()` | [Java 8+ の例](src/test/java/io/confluent/examples/streams/AggregateTest.java) | | [Scala の例](src/test/scala/io/confluent/examples/streams/AggregateScalaTest.scala) |
| CustomStreamTableJoin | DSL、Processor API、トランスフォーマー | [Java 8+ の例](src/test/java/io/confluent/examples/streams/CustomStreamTableJoinIntegrationTest.java) | | |
| EventDeduplication | DSL、Processor API、トランスフォーマー | [Java 8+ の例](src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java) | | |
| GlobalKTable   | DSL、グローバルステート | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/GlobalKTablesExampleTest.java) | |
| GlobalStore    | DSL、グローバルステート、トランスフォーマー | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/GlobalStoresExampleTest.java) | |
| HandlingCorruptedInputRecords | DSL、`flatMap()` | [Java 8+ の例](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java) | | |
| KafkaMusic (インタラクティブクエリ) | インタラクティブクエリ、ステートストア、REST API | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExampleTest.java) | |
| MapFunction     | DSL、ステートレス変換、`map()` | [Java 8+ の例](src/test/java/io/confluent/examples/streams/MapFunctionLambdaIntegrationTest.java) | | |
| MixAndMatch DSL + Processor API | DSL と Processor API の統合 | [Java 8+ の例](src/test/java/io/confluent/examples/streams/MixAndMatchLambdaIntegrationTest.java) | | |
| PassThrough     | DSL、`stream()`、`to()` | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/PassThroughIntegrationTest.java) | |
| PoisonPill                          | DSL、`flatMap()`                            | [Java 8+ の例](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java) | | |
| ProbabilisticCounting\*\*\*         | DSL、Processor API、カスタムステートストア     | | | [Scala の例](src/test/scala/io/confluent/examples/streams/ProbabilisticCountingScalaIntegrationTest.scala) |
| Reduce (Concatenate)                | DSL、`groupByKey()`、`reduce()`             | [Java 8+ の例](src/test/java/io/confluent/examples/streams/ReduceTest.java) | | [Scala の例](src/test/scala/io/confluent/examples/streams/ReduceScalaTest.scala) |
| SessionWindows                      | DSL、ウィンドウ付き集計、セッショニゼーション   | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/SessionWindowsExampleTest.java) | |
| StatesStoresDSL                     | DSL、Processor API、トランスフォーマー            | [Java 8+ の例](src/test/java/io/confluent/examples/streams/StateStoresInTheDSLIntegrationTest.java) | | |
| StreamToStreamJoin                  | DSL、KStream 間の `join()`                   | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/StreamToStreamJoinIntegrationTest.java) | |
| StreamToTableJoin                   | DSL、KStream 間の `join()`                   | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/StreamToTableJoinIntegrationTest.java) | [Scala の例](src/test/scala/io/confluent/examples/streams/StreamToTableJoinScalaIntegrationTest.scala) |
| Sum                                 | DSL、集計、ステートフル処理、`reduce()`      | [Java 8+ の例](src/test/java/io/confluent/examples/streams/SumLambdaIntegrationTest.java) | | |
| TableToTableJoin                    | DSL、KTable 間の `join()`                   | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/TableToTableJoinIntegrationTest.java) | |
| UserCountsPerRegion                 | DSL、集計、ステートフル処理、`count()`       | [Java 8+ の例](src/test/java/io/confluent/examples/streams/UserCountsPerRegionLambdaIntegrationTest.java) | | |
| ValidateStateWithInteractiveQueries | ステートを検証するためのインタラクティブクエリ | | [Java 8+ の例](src/test/java/io/confluent/examples/streams/ValidateStateWithInteractiveQueriesLambdaIntegrationTest.java) | | |
| GenericAvro                         | Generic Avro 形式のデータの扱い            | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/GenericAvroIntegrationTest.java) |  [Scala の例](src/test/scala/io/confluent/examples/streams/GenericAvroScalaIntegrationTest.scala) |
| SpecificAvro                        | Specific Avro形式のデータを扱う方法     | | [Java 7+ の例](src/test/java/io/confluent/examples/streams/SpecificAvroIntegrationTest.java) | [Scalaの例](src/test/scala/io/confluent/examples/streams/SpecificAvroScalaIntegrationTest.scala) |

***カスタムな状態ストア（[CMSStore](src/main/scala/io/confluent/examples/streams/algebird/CMSStore.scala)）を実装することで、[Count-Min Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch)データ構造（[Twitter Algebird](https://github.com/twitter/algebird)のCMS実装を使用）をバックエンドとして、入力ストリーム内のアイテムを確率的にカウントする方法を示しています。***


<a name="examples-docker"/>

# Docker例：Kafka Musicデモアプリケーション

このコンテナ化された例では、以下が起動します：

* Kafka Streams API向けのConfluent製Kafka Musicデモアプリケーションで、[Interactive Queries](http://docs.confluent.io/current/streams/developer-guide.html)を利用している  
* 単一ノード構成のApache Kafkaクラスターおよび単一ノード構成のZooKeeperセットアップ  
* [Confluent Schema Registry](https://github.com/confluentinc/schema-registry)インスタンス

Kafka Musicアプリケーションは、各音楽ジャンルの最新トップ5曲などの最新チャートをリアルタイムで継続的に算出する、シンプルな音楽チャートアプリケーションの構築方法を示しています。このアプリケーションは、Kafkaの[Interactive Queries](http://docs.confluent.io/current/streams/developer-guide.html#interactive-queries)機能を通じてREST APIを利用し、最新の処理結果である最新チャートを公開します。アプリケーションの入力データはAvro形式であるためConfluent Schema Registryが使用されており、そのデータは「曲Xが再生された」といった再生イベントのストリームや、「曲XはアーティストYによって制作された」といった曲のメタデータのストリームという2つの出所から得られます。

詳細なドキュメントは
https://docs.confluent.io/current/streams/kafka-streams-examples/docs/index.html
でご覧いただけます。


#a 要件事項
## Apache Kafka

# 例：イベントストリーミングプラットフォーム

イベントストリーミングプラットフォーム上でのKafka Streamsアプリケーションを示すその他の例については、[examples GitHubリポジトリ](https://github.com/confluentinc/examples)をご覧ください。


#a 要件
<a name="requirements-kafka"/>

# 要件

<a name="requirements-kafka"/>

## Apache Kafka

このリポジトリのコードにはApache Kafka 0.10以降が必要です。というのも、このバージョン以降のKafkaにはその[Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams)ライブラリが含まれているからです。このリポジトリの異なるブランチによってKafkaの要件が異なる場合があるため、詳細については[バージョン互換性マトリックス](#version-compatibility)をご覧ください。

# masterブランチの場合：  
開発版をビルドするには、通常、Apache Kafkaの最新のtrunkバージョンが必要です（詳細については[pom.xml](pom.xml)内の`kafka.version`を参照してください）。以下の手順に従えば、最新のtrunkバージョンのKafkaをビルドし、ローカルにインストールできます：  

```shell
> $ git clone git@github.com:apache/kafka.git
> $ cd kafka
> $ git checkout trunk
>
> # ここでKafkaをローカルでビルドし、インストールする
> $./gradlew clean &&./gradlewAll install
> ```


<a name="requirements-confluent-platform"/>

## Confluent Platform

このリポジトリのコードには[Confluent Schema Registry](https://github.com/confluentinc/schema-registry)が必要です。  
このリポジトリの各ブランチで求められるConfluent Platformの要件が異なるため、詳細については[バージョン互換性マトリックス](#version-compatibility)をご覧ください。

* [Confluent Platform Quickstart](http://docs.confluent.io/current/quickstart.html)（ダウンロードおよびインストール方法）
* [Confluent Platformドキュメント](http://docs.confluent.io/current/)

> **`master` ブランチの場合:** 開発版をビルドするには、通常、Confluent Platform の Schema Registry の最新 `master` バージョンが必要です（[pom.xml](pom.xml) 内の `confluent.version` を参照してください。この値は上流の [Confluent Common](https://github.com/confluentinc/common) プロジェクトによって設定されます）。  
> 以下の手順では、最新の `master` バージョンの Schema Registry をビルドしローカルにインストールします。これには [Confluent Common](https://github.com/confluentinc/common) や [Confluent Rest Utils](https://github.com/confluentinc/rest-utils) といった依存関係のビルドも含まれます。詳細については [Schema Registry README](https://github.com/confluentinc/schema-registry) をご覧ください。  
> 
> ```shell
> $ git clone https://github.com/confluentinc/common.git
> $ cd common
> $ git checkout master
>
> # common をローカルでビルドしインストール
> $ mvn -DskipTests=true clean install
>
> $ git clone https://github.com/confluentinc/rest-utils.git
> $ cd rest-utils
> $ git checkout master
>
> # rest-utils をローカルでビルドしインストール
> $ mvn -DskipTests=true clean install
>
> $ git clone https://github.com/confluentinc/schema-registry.git
> $ cd schema-registry
> $ git checkout master
>
> # ここで schema-registry をローカルでビルドしインストール
> $ mvn -DskipTests=true clean install
> ```

また、各例は最上部にその実行に必要な正確な要件を記載しています。


<a name="requirements-ide"/>

## IntelliJ または Eclipse の使用方法

IDEを使用してプロジェクトをインポートすると、「importが見つかりません／クラスが見つかりません」というエラーが発生することがあります。
一部のAvroクラスはスキーマファイルから生成されるもので、IDEは必ずしもこれらのクラスを自動的に生成しない場合があります。
このエラーを解決するには、手動で次のコマンドを実行してください：

```shell
$ mvn -DskipTests=true compile
```

Eclipseを使用している場合は、`pom.xml`ファイルを右クリックし、「Run As」>「Maven generate-sources」を選択することもできます。


<a name="requirements-java"/>

## Java 17+

IntelliJ IDEAを使用している方へ：

* _File > Project structure_ を開く
* 左側で「Project」を選択する。
    * 「Project SDK」をJava 17に設定する。
    * 「Project language level」を「17 - Sealed types, always-strict floating-point semantics」に設定する。


<a name="requirements-scala"/>

## Scala

このリポジトリにあるScalaのサンプルを試してみたい場合にのみScalaが必要です。Java開発者であれば、このセクションは安全に無視して構いません。

このリポジトリにあるScalaのサンプルを試してみたい場合は、Java 17をサポートするScalaバージョンが必要です。

<a name="packaging-and-running"/>

# アプリケーション例のパッケージングと実行

このセクションの手順は、[src/main/](src/main/) 内にある[アプリケーション例](#examples-apps)をインタラクティブに試してみたい場合にのみ必要です。

> **ヒント:** 統合テストのみを実行したい場合（`mvn test`）、何もパッケージ化したりインストールしたりする必要はなく、単に`mvn test`を実行すればよいです。これらのテストでは組み込みのKafkaクラスターが起動します。

まず最初のステップとして、Kafkaクラスターをインストールして起動する必要があります。このクラスターには少なくとも1つのKafkaブローカーと、少なくとも1つのZooKeeperインスタンスが含まれていなければなりません。また、一部のサンプルでは稼働中のConfluent Schema Registryインスタンスも必要になる場合があります。詳細については、[Confluent Platform Quickstart](http://docs.confluent.io/current/quickstart.html)ガイドをご覧ください。

要約すると：

```shell
# 上記のクイックスタートガイドに従って、Confluent Platformをダウンロードしインストールしていることを確認してください。

# クラスターのUUIDを生成する
$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# ログディレクトリをフォーマットする
$ bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/kraft/reconfig-server.properties

# Kafkaブローカーを起動する
$./bin/kafka-server-start./etc/kafka/server.properties

# 別のターミナルで、Confluent Schema Registryを起動する
$./bin/schema-registry-start./etc/schema-registry/schema-registry.properties

# Confluent Platformのダウンロード方法や上記3つのサービスの停止方法などの詳細については、引き続きConfluent Platformクイックスタートガイドを参照してください。
```

次のステップは、[アプリケーション例](#examples-apps)のスタンドアロン jar（「fat jar」）を作成することです：

```shell
# スタンドアロンジャー（“fat jar”）を作成する
$ mvn clean package

# >>> target/kafka-streams-examples-8.4.0-0-standalone.jar が作成される
```

ヒント：必要に応じて、パッケージング時にテストスイートを無効にすることもできます。例えば、パッケージングの速度を上げたり、JVMのメモリ使用量を削減したりするためです：
>
> ```shell
> $ mvn -DskipTests=true clean package
> ```

これで、以下のようにアプリケーション例を実行できます：

```shell
# スタンドアロンジャーからサンプルアプリケーションを実行します。ここでは `WordCountLambdaExample` を使用します。
$ java -cp target/kafka-streams-examples-8.4.0-0-standalone.jar \
  io.confluent.examples.streams.WordCountLambdaExample
```

アプリケーションは、指定された入力トピック（上記の例では「streams-plaintext-input」）からデータを読み取り、処理ロジックを実行した後、指定された出力トピック（上記の例では「streams-wordcount-output」）にデータを書き戻そうとします。期待される出力ストリームを確認するには、入力トピックにメッセージを送信するためのコンソールプロデューサーと、出力トピックから継続的にデータを読み取るためのコンソールコンシューマーを起動する必要があります。各サンプルコードの[javaドキュメント](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java#L31)に、サンプルの実行方法に関するより詳細な情報が記載されています。

例アプリケーションを実行しながらlog4j2を有効にしたい場合は、
[log4j2.yaml](src/main/resources/log4j2.yaml)ファイルを編集し、次のように実行します：

```shell
# スタンドアロンジャーファイルからサンプルアプリケーションを実行します。ここでは `WordCountLambdaExample` を使用します。
$ java -cp target/kafka-streams-examples-8.4.0-0-standalone.jar \
  -Dlog4j2.configurationFile=src/main/resources/log4j2.yaml \
  io.confluent.examples.streams.WordCountLambdaExample
```

上記のコマンドを実行するマシンは、コード例で設定したKafka/ZooKeeperクラスターにアクセスできる必要があることにご注意ください。デフォルトでは、コード例はKafkaクラスターが`localhost:9092`（Kafkaの「bootstrap.servers」パラメータ）経由で、ZooKeeperアンサンブルが`localhost:2181`経由でアクセス可能であると仮定しています。デフォルトの「bootstrap.servers」パラメータは、コマンドライン引数を通じて上書きすることができます。


#a 开发</a>

# 開発

このプロジェクトでは、標準的なMavenライフサイクルおよび以下のようなコマンドが使用されています：

```shell
$ mvn compile # AvroスキーマからJavaクラスも生成されます
$ mvn test    # 単体テストおよび統合テストを実行します
$ mvn package # アプリケーション例をスタンドアロンjarにパッケージ化します
```


<a name="version-compatibility"/>

# バージョン互換性マトリックス

| ブランチ（このリポジトリ）                | Confluent Platform | Apache Kafka      |
| ----------------------------------------| --------------------|-------------------|
| [master](../../../tree/master/)\*       | 8.0.0-SNAPSHOT     | 4.0.0-SNAPSHOT    |
| [7.9.x](../../../tree/7.9.x/)           | 7.9.0-SNAPSHOT     | 3.9.0             |
| [7.8.0-post](../../../tree/7.8.0-post/) | 7.8.0              | 3.8.0             |
|...                                     |                    |                   |
| [7.1.0-post](../../../tree/7.1.0-post/) | 7.1.0              | 3.1.0             |

7.1.0より前の古いバージョンは、[もはやサポートされていません](https://docs.confluent.io/platform/current/installation/versions-interoperability.html)。

\*Apache Kafkaの`4.0`バージョンおよびConfluent Platformの`8.0.0`バージョンは、手動でビルドする必要があります。上記の手順を参照してください。

このリポジトリの`master`ブランチは現在開発が進行中であり、コンパイルさせるためにはユーザー側で追加の手順が必要になる場合があります。そのような情報については、このREADMEや[pom.xml](pom.xml)をご確認ください。


<a name="help"/>

# ヘルプの探し方

* Apache KafkaのStreams APIに関するドキュメントをお探しですか？
    * [Confluent Platformドキュメント](https://docs.confluent.io/current/)内の
      [Kafka Streamsの章](https://docs.confluent.io/current/streams/)をご覧になることをお勧めします。
    * 当社の講演
      [Rethinking Stream Processing with Apache Kafka](https://www.youtube.com/watch?v=ACwnrnVJXuE)もご覧ください。
* このプロジェクトにあるデモやサンプルの使用で問題に直面していますか？
    * まずは当社の[FAQ Wiki](https://github.com/confluentinc/kafka-streams-examples/wiki/FAQ)で回答を確認してください。
    * FAQでも解決しない場合は、[新しいGitHub Issueを作成してください](https://github.com/confluentinc/kafka-streams-examples/issues)。
* 質問がある、KafkaまたはそのKafka Streams APIのバグを報告したい、新しいKafka機能をリクエストしたいですか？
    * Apache KafkaおよびConfluent Platformに関する一般的な質問は、
      [Confluentメーリングリスト](https://groups.google.com/forum/?pli=1#!forum/confluent-platform)
      または [Apache Kafkaメーリングリスト](http://kafka.apache.org/contact)にお問い合わせください。

# ライセンス

このイメージの使用は、内部に含まれるソフトウェアのライセンス条件に従います。詳細については、ConfluentのDockerイメージに関するドキュメント[参照](https://docs.confluent.io/platform/current/installation/docker/image-reference.html)をご覧ください。カスタムDockerイメージを拡張し作成するためのソフトウェアは、Apache 2.0ライセンスの下で提供されています。
