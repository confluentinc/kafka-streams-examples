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
package io.confluent.examples.streams.interactivequeries.kafkamusic;

import io.confluent.examples.streams.ExampleTestUtils;
import io.confluent.examples.streams.avro.PlayEvent;
import io.confluent.examples.streams.avro.Song;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test for {@link KafkaMusicExample}. Demonstrates
 * how you can programmatically query the REST API exposed by {@link MusicPlaysRestService}
 */
public class KafkaMusicExampleTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  // A mocked schema registry for our serdes to use
  private static final String SCHEMA_REGISTRY_SCOPE = KafkaMusicExample.class.getName();
  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

  private static final int MAX_WAIT_MS = 30000;
  private KafkaStreams streams;
  private MusicPlaysRestService restProxy;
  private int appServerPort;
  private static final List<Song> songs = new ArrayList<>();
  private static final Logger log = LoggerFactory.getLogger(KafkaMusicExampleTest.class);

  @BeforeClass
  public static void createTopicsAndProduceDataToInputTopics() throws Exception {
    CLUSTER.createTopic(KafkaMusicExample.PLAY_EVENTS);
    CLUSTER.createTopic(KafkaMusicExample.SONG_FEED);
    // these topics initialized just to avoid some rebalances.
    // they would normally be created by KafkaStreams.
    CLUSTER.createTopic("kafka-music-charts-song-play-count-changelog");
    CLUSTER.createTopic("kafka-music-charts-song-play-count-repartition");
    CLUSTER.createTopic("kafka-music-charts-top-five-songs-by-genre-changelog");
    CLUSTER.createTopic("kafka-music-charts-top-five-songs-by-genre-repartition");
    CLUSTER.createTopic("kafka-music-charts-top-five-songs-changelog");
    CLUSTER.createTopic("kafka-music-charts-top-five-songs-repartition");
    CLUSTER.createTopic("kafka-music-charts-KSTREAM-MAP-0000000004-repartition");

    // Read comma-delimited file of songs into Array
    final String SONGFILENAME = "song_source.csv";
    final InputStream inputStream = KafkaMusicExample.class.getClassLoader().getResourceAsStream(SONGFILENAME);
    final InputStreamReader streamReader = new InputStreamReader(inputStream, UTF_8);
    try (final BufferedReader br = new BufferedReader(streamReader)) {
      String line;
      while ((line = br.readLine()) != null) {
        final String[] values = line.split(",");
        final Song newSong = new Song(Long.parseLong(values[0]), values[1], values[2], values[3], values[4]);
        songs.add(newSong);
      }
    }

    // Produce sample data to the input topic before the tests starts.
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL
    );
    final SpecificAvroSerializer<PlayEvent> playEventSerializer = new SpecificAvroSerializer<>();
    playEventSerializer.configure(serdeConfig, false);

    final SpecificAvroSerializer<Song> songSerializer = new SpecificAvroSerializer<>();
    songSerializer.configure(serdeConfig, false);

    final KafkaProducer<String, PlayEvent> playEventProducer = new KafkaProducer<>(props,
        Serdes.String().serializer(),
        playEventSerializer);

    final KafkaProducer<Long, Song> songProducer = new KafkaProducer<>(props,
        new LongSerializer(),
        songSerializer);

    songs.forEach(song -> songProducer.send(
        new ProducerRecord<Long, Song>(KafkaMusicExample.SONG_FEED,
            song.getId(),
            song)));

    songProducer.flush();
    songProducer.close();

    // create the play events we can use for charting
    sendPlayEvents(6, songs.get(0), playEventProducer);
    sendPlayEvents(5, songs.get(1), playEventProducer);
    sendPlayEvents(4, songs.get(2), playEventProducer);
    sendPlayEvents(3, songs.get(3), playEventProducer);
    sendPlayEvents(2, songs.get(4), playEventProducer);
    sendPlayEvents(1, songs.get(5), playEventProducer);

    sendPlayEvents(6, songs.get(6), playEventProducer);
    sendPlayEvents(5, songs.get(7), playEventProducer);
    sendPlayEvents(4, songs.get(8), playEventProducer);
    sendPlayEvents(3, songs.get(9), playEventProducer);
    sendPlayEvents(2, songs.get(10), playEventProducer);
    sendPlayEvents(1, songs.get(11), playEventProducer);

    playEventProducer.close();
  }

  @AfterClass
  public static void cleanup() {
    MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
  }

  private void createStreams(final String host) throws Exception {
    appServerPort = ExampleTestUtils.randomFreeLocalPort();

    final Map<String, String> serdeConfig = Collections.singletonMap(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL
    );
    streams = new KafkaStreams(
      KafkaMusicExample.buildTopology(serdeConfig),
      KafkaMusicExample.streamsConfig(
        CLUSTER.bootstrapServers(),
        appServerPort,
        TestUtils.tempDirectory().getPath(),
        host
      )
    );
    int count = 0;
    final int maxTries = 3;
    while (count <= maxTries) {
      try {
        // Starts the Rest Service on the provided host:port
        restProxy = KafkaMusicExample.startRestProxy(streams, new HostInfo(host, appServerPort));
        break;
      } catch (final Exception ex) {
        log.error("Could not start Rest Service due to: " + ex.toString());
      }
      count++;
    }
  }

  @After
  public void shutdown() {
    try {
      restProxy.stop();
    } catch (final Exception e) {
      log.error(String.format("Error while stopping restproxy: %s",e.getMessage()));
    }
    streams.close();
  }

  @Test
  public void shouldCreateChartsAndAccessThemViaInteractiveQueries() throws Exception {
    final String host = "localhost";
    createStreams(host);
    final CountDownLatch startupLatch = new CountDownLatch(1);
    streams.setStateListener((newState, oldState) -> {
      if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
        startupLatch.countDown();
      }
    });
    streams.start();

    try {
      if (!startupLatch.await(60, TimeUnit.SECONDS)) {
        throw new RuntimeException("Streams never finished rebalancing on startup");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (restProxy != null) {
      // wait until the StreamsMetadata is available as this indicates that
      // KafkaStreams initialization has occurred
      TestUtils.waitForCondition(
        () -> !StreamsMetadata.NOT_AVAILABLE.equals(streams.allMetadataForStore(KafkaMusicExample.TOP_FIVE_SONGS_STORE)),
        MAX_WAIT_MS,
        "StreamsMetadata should be available");
      final String baseUrl = "http://" + host + ":" + appServerPort + "/kafka-music";
      final Client client = ClientBuilder.newClient();
  
      // Wait until the all-songs state store has some data in it
      TestUtils.waitForCondition(() -> {
        final ReadOnlyKeyValueStore<Long, Song> songsStore;
        try {
          songsStore = streams.store(KafkaMusicExample.ALL_SONGS, QueryableStoreTypes.keyValueStore());
          return songsStore.all().hasNext();
        } catch (final Exception e) {
          e.printStackTrace();
          return false;
        }
      }, MAX_WAIT_MS, KafkaMusicExample.ALL_SONGS + " should be non-empty");
  
      final IntFunction<SongPlayCountBean> intFunction = index -> {
        final Song song = songs.get(index);
        return songCountPlayBean(song, 6L - (index % 6));
      };
  
      // Verify that the charts are as expected
      verifyChart(baseUrl + "/charts/genre/punk",
                  client,
                  IntStream.range(0, 5).mapToObj(intFunction).collect(Collectors.toList()));
  
      verifyChart(baseUrl + "/charts/genre/hip hop",
                  client,
                  IntStream.range(6, 11).mapToObj(intFunction).collect(Collectors.toList()));
  
      verifyChart(baseUrl + "/charts/top-five",
                  client,
                  Arrays.asList(songCountPlayBean(songs.get(0), 6L),
                                songCountPlayBean(songs.get(6), 6L),
                                songCountPlayBean(songs.get(1), 5L),
                                songCountPlayBean(songs.get(7), 5L),
                                songCountPlayBean(songs.get(2), 4L)
                                )
                  );
  
    } else {
      fail("Should fail demonstrating InteractiveQueries as the Rest Service failed to start.");
    }
  }

  private SongPlayCountBean songCountPlayBean(final Song song, final long plays) {
    return new SongPlayCountBean(song.getArtist(),
                                 song.getAlbum(),
                                 song.getName(),
                                 plays);
  }

  private void verifyChart(final String url,
                           final Client client,
                           final List<SongPlayCountBean> expectedChart) throws InterruptedException {
    final Invocation.Builder genreChartRequest = client
      .target(url)
      .request(MediaType.APPLICATION_JSON_TYPE);

    TestUtils.waitForCondition(() -> {
      try {
        final List<SongPlayCountBean> chart = MicroserviceTestUtils.getWithRetries(
            genreChartRequest,
            new GenericType<List<SongPlayCountBean>>() {},
            0);
        System.err.println(chart.size());
        return chart.size() == 5;
      } catch (final Exception e) {
        e.printStackTrace();
        return false;
      }
    }, MAX_WAIT_MS, "chart should have 5 items");

    final List<SongPlayCountBean> chart = MicroserviceTestUtils.getWithRetries(
      genreChartRequest,
      new GenericType<List<SongPlayCountBean>>() {},
      5);
    assertThat(chart, is(expectedChart));
  }

  private static void sendPlayEvents(final int count,
                                     final Song song,
                                     final KafkaProducer<String, PlayEvent> producer) {
    for (int i = 0; i < count; i++) {
      producer.send(new ProducerRecord<>(
          KafkaMusicExample.PLAY_EVENTS,
          "UK",
          new PlayEvent(song.getId(), 60000L)));
    }
    producer.flush();
  }

}
