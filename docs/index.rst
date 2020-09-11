.. _kafka-streams-example-music:

|ak| Streams Demo Application
-----------------------------

This demo showcases |ak-tm| Streams API (:cp-examples:`source code|src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java`) and ksqlDB (see blog post `Hands on: Building a Streaming Application with KSQL <https://www.confluent.io/blog/building-streaming-application-ksql/>`__ and video `Demo: Build a Streaming Application with ksqlDB(https://www.youtube.com/watch?v=ExEWJVjj-RA`__).

The music application demonstrates how to build a simple music charts application that continuously computes,
in real-time, the latest charts such as Top 5 songs per music genre.  It exposes its latest processing results -- the
latest charts -- via the |ak| :ref:`Interactive Queries <streams_developer-guide_interactive-queries>` feature and a REST
API.  The application's input data is in Avro format and comes from two sources: a stream of play events (think: "song
X was played") and a stream of song metadata ("song X was written by artist Y").

The following screencast shows a live bit of the music demo application:

.. raw:: html

  <p>
  <a href="https://asciinema.org/a/111755">
    <img src="https://asciinema.org/a/111755.png" width="400" />
  </a>
  </p>
  <p>
    <a href="https://asciinema.org/a/111755">
      <strong>Screencast: Running Confluent's Kafka Music demo application (3 mins)</strong>
    </a>
  </p>

Prerequisites
~~~~~~~~~~~~~

.. include:: ../../../tutorials/examples/docs/includes/demo-validation-env.rst


Start the music application
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run this demo, complete the following steps:

#. Clone the Confluent examples repository:

   .. code-block:: bash

       git clone https://github.com/confluentinc/examples.git

#. Navigate to the ``examples/music/`` directory and switch to the |cp| release branch:

   .. codewithvars:: bash

       cd examples/music/
       git checkout |release_post_branch|

#. Start the demo in one of two modes, depending on whether you are running in Docker or |cp| locally:

   * Docker: run the full solution using ``docker-compose`` (this also starts a local |cp| cluster in Docker containers).

     .. sourcecode:: bash

        ./start-docker.sh

   * Local: run the full solution using the provided script (this also starts a local |cp| cluster using Confluent CLI):

     .. sourcecode:: bash

        ./start.sh

Once the demo is running, note the available endpoints from within the containers and from your host machine:

+---------------------------+-------------------------+---------------------------------+--------------------------------+
| Endpoint                  | Parameter               | Value (from within containers)  | Value (from your host machine) |
+===========================+=========================+=================================+================================+
| |ak|  Cluster             | ``bootstrap.servers``   | ``kafka:29092``                 | ``localhost:9092``             |
+---------------------------+-------------------------+---------------------------------+--------------------------------+
| |sr-long|                 | ``schema.registry.url`` | ``http://schema-registry:8081`` | ``http://localhost:8081``      |
+---------------------------+-------------------------+---------------------------------+--------------------------------+
| |zk|                      | ``zookeeper.connect``   | ``zookeeper:32181``             | ``localhost:32181``            |
+---------------------------+-------------------------+---------------------------------+--------------------------------+


Validate the application
~~~~~~~~~~~~~~~~~~~~~~~~

After a few seconds the application and the services are up and running.  One of the started containers is continuously
generating input data for the application by writing into its input topics.  This allows us to look at live, real-time
data when playing around with the |ak| Music application.

Now you can use your web browser or a CLI tool such as ``curl`` to interactively query the latest processing results of
the |ak| Music application by accessing its REST API.

#. List all running application instances of the |ak| Music application.

   ::
  
     curl -sXGET http://localhost:7070/kafka-music/instances | jq .

#. Verify your output resembles:

   ::
  
       [
         {
           "host": "localhost",
           "port": 7070,
           "storeNames": [
             "all-songs",
             "song-play-count",
             "top-five-songs",
             "top-five-songs-by-genre"
           ]
         }
       ]

#. Get the latest Top 5 songs across all music genres

   ::

     curl -sXGET http://localhost:7070/kafka-music/charts/top-five

#. Verify your output resembles:

   ::
     
       [
         {
           "artist": "Jello Biafra And The Guantanamo School Of Medicine",
           "album": "The Audacity Of Hype",
           "name": "Three Strikes",
           "plays": 70
         },
         {
           "artist": "Hilltop Hoods",
           "album": "The Calling",
           "name": "The Calling",
           "plays": 67
         },
         ...
       ]

#. The REST API exposed by the :cp-examples:`Kafka Music application|src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java` supports further operations.  See the :cp-examples:`top-level instructions in its source code|src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java` for details.

Inspect |ak| topics
~~~~~~~~~~~~~~~~~~~

#. Inspect the ``play-events`` input topic, which contains messages in Avro format:

   .. codewithvars:: bash
  
       $ docker-compose exec schema-registry \
           kafka-avro-console-consumer \
               --bootstrap-server kafka:29092 \
               --topic play-events --from-beginning
   
#. Verify your output resembles:

   .. codewithvars:: bash

       # You should see output similar to:
       {"song_id":11,"duration":60000}
       {"song_id":10,"duration":60000}
       {"song_id":12,"duration":60000}
       {"song_id":2,"duration":60000}
       {"song_id":1,"duration":60000}

#. Inspect the ``song-feed`` input topic, which contains messages in Avro format:

   .. codewithvars:: bash
  
       # Use the kafka-avro-console-consumer to read the "song-feed" topic
       $ docker-compose exec schema-registry \
           kafka-avro-console-consumer \
               --bootstrap-server kafka:29092 \
               --topic song-feed --from-beginning

#. Verify your output resembles:

   ::
  
     {"id":1,"album":"Fresh Fruit For Rotting Vegetables","artist":"Dead Kennedys","name":"Chemical Warfare","genre":"Punk"}
     {"id":2,"album":"We Are the League","artist":"Anti-Nowhere League","name":"Animal","genre":"Punk"}
     {"id":3,"album":"Live In A Dive","artist":"Subhumans","name":"All Gone Dead","genre":"Punk"}
     {"id":4,"album":"PSI","artist":"Wheres The Pope?","name":"Fear Of God","genre":"Punk"}


Stop the music application
~~~~~~~~~~~~~~~~~~~~~~~~~~


#. When you are done, make sure to stop the demo.

   * Docker:

     .. sourcecode:: bash

        docker-compose down

   * Local:

     .. sourcecode:: bash

        ./stop.sh

