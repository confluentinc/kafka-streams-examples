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

package io.confluent.examples.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import javax.ws.rs.GET;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class BankingApplication {

  private final KafkaStreams kafkaStreams;

  public BankingApplication(final KafkaStreams kafkaStreams) {
    this.kafkaStreams = kafkaStreams;
  }

  @GET("/summary")
  public JsonNode summary(String accountId) {
    final CurrentBalance balance = fetchCurrentBalance(accountId);
    return JsonNodeFactory.instance.objectNode()
        .put("balance", balance.getBalance())
        .put("last purchase", balance.getLastPurchase())
        ;
  }

  public static void main(String[] args) {
    final HostInfo hostInfo = new HostInfo("10.0.0.123", 1234);

    final Properties config = new Properties();
    // The connection to Kafka
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Our instances form a cluster simply by using the same key here
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "banking-app");
    // We can tell Streams what IP address we're listening for peer connections on
    config.setProperty(
        StreamsConfig.APPLICATION_SERVER_CONFIG,
        hostInfo.host() + ":" + hostInfo.port()
    );

    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()))
        .groupByKey()
        .reduce((currentT, newT) -> {
          long newAmount = currentT.amount + newT.amount;
          String lastPurchase = newT.product;
          return new Transaction(newAmount, lastPurchase);
        });

    final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);

    final BankingApplication bankingApplication = new BankingApplication(kafkaStreams);
    bankingApplication.run();
  }

  private void run() {
    final CountDownLatch startupLatch = new CountDownLatch(1);
    kafkaStreams.setStateListener(new StateListener() {
      @Override
      public void onChange(final State oldState, final State newState) {
        if (newState == State.RUNNING) {
          startupLatch.countDown();
        }
      }
    });
    kafkaStreams.start();
    // wait for Streams to reach RUNNING state (at which point, it can serve IQ)
    startupLatch.await();
    // then start REST server
    // ...
  }

  private CurrentBalance fetchCurrentBalance(final String accountId) {
    final KeyQueryMetadata metadata =
        kafkaStreams.queryMetadataForKey(
            "currentBalance",
            accountId,
            new StringSerializer()
        );
    final HostInfo hostInfo = metadata.activeHost();

    if (isLocal(hostInfo)) {
      final ReadOnlyKeyValueStore<String, ValueAndTimestamp<CurrentBalance>> currentBalanceStore =
          kafkaStreams.store(
              StoreQueryParameters.fromNameAndType(
                  "currentBalance",
                  QueryableStoreTypes.timestampedKeyValueStore()
              )
          );
      return currentBalanceStore.get(accountId).value();
    } else {
      return forwardRequestTo(hostInfo, accountId);
    }
  }

  private CurrentBalance forwardRequestTo(final HostInfo hostInfo, final String accountId) {
    // dummy: would do a real web request to the correct host.
    throw new RuntimeException("placeholder for request forwarding");
  }

  private boolean isLocal(final HostInfo hostInfo) {
  }

  public static class CurrentBalance {

    private final int balance;
    private final String lastPurchase;

    public CurrentBalance(final int balance, final String lastPurchase) {
      this.balance = balance;
      this.lastPurchase = lastPurchase;
    }

    public String getLastPurchase() {
      return lastPurchase;
    }

    public int getBalance() {
      return balance;
    }
  }

  public static class Transaction {

    private final int amount;
    private final String product;

    public Transaction(final int amount, final String product) {
      this.amount = amount;
      this.product = product;
    }

    public int getAmount() {
      return amount;
    }

    public String getProduct() {
      return product;
    }
  }

  public static class TransactionSerde implements Serde<Transaction>, Serializer<Transaction>,
      Deserializer<Transaction> {

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      Serde.super.configure(configs, isKey);
    }

    @Override
    public void close() {
      Serde.super.close();
    }

    @Override
    public Transaction deserialize(final String s, final byte[] bytes) {
      return null;
    }

    @Override
    public Serializer<Transaction> serializer() {
      return null;
    }

    @Override
    public Deserializer<Transaction> deserializer() {
      return null;
    }

    @Override
    public byte[] serialize(final String s, final Transaction transaction) {
      return new byte[0];
    }
  }
}
