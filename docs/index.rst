.. _kafka-streams-example-music:

|ak| Streams Demo Application
-----------------------------

This demo showcases |ak-tm| Streams API (:cp-examples:`source code|src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java`) and ksqlDB (see blog post `Hands on: Building a Streaming Application with KSQL <https://www.confluent.io/blog/building-streaming-application-ksql/>`__ and video `Demo: Build a Streaming Application with ksqlDB <https://www.youtube.com/watch?v=ExEWJVjj-RA>`__).

The music application demonstrates how to build a simple music charts application that continuously computes,
in real-time, the latest charts such as Top 5 songs per music genre.  It exposes its latest processing results -- the
latest charts -- via the |ak| :ref:`Interactive Queries <streams_developer-guide_interactive-queries>` feature and a REST
API.  The application's input data is in Avro format and comes from two sources: a stream of play events (think: "song
X was played") and a stream of song metadata ("song X was written by artist Y").

The following `screencast <https://asciinema.org/a/111755>`__ shows a live bit of the music demo application:

.. raw:: html

  <p>
  <a href="https://asciinema.org/a/111755">
    <img src="https://asciinema.org/a/111755.png" width="400" />
  </a>
  </p>

Prerequisites
~~~~~~~~~~~~~

.. include:: ../../../tutorials/examples/docs/includes/demo-validation-env.rst


Start the music application
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run this demo, complete the following steps:

#. In Docker's advanced `settings <https://docs.docker.com/docker-for-mac/#advanced>`__, increase the memory dedicated to Docker to at least 8GB (default is 2GB).

#. Clone the Confluent examples repository:

   .. code-block:: bash

       git clone https://github.com/confluentinc/examples.git

#. Navigate to the ``examples/music/`` directory and switch to the |cp| release branch:

   .. codewithvars:: bash

       cd examples/music/
       git checkout |release_post_branch|

#. Start the demo by running a single command that brings up all the Docker containers. This takes about 2 minutes to complete.

   .. code-block:: bash

      docker-compose up -d

#. View the |c3| logs and validate that it is running.

   .. code-block:: bash

      docker-compose logs -f control-center | grep -i "Started NetworkTrafficServerConnector"

#. Verify that you see in the |c3| logs:

   .. code-block:: bash

      INFO Started NetworkTrafficServerConnector@5533dc72{HTTP/1.1,[http/1.1]}{0.0.0.0:9021} (org.eclipse.jetty.server.AbstractConnector)

#. Note the available endpoints of the |ak| brokers, |sr-long|, and |zk|, from within the containers and from your host machine:

   +---------------------------+-------------------------+---------------------------------+--------------------------------+
   | Endpoint                  | Parameter               | Value (from within containers)  | Value (from host machine)      |
   +===========================+=========================+=================================+================================+
   | |ak| brokers              | ``bootstrap.servers``   | ``kafka:29092``                 | ``localhost:9092``             |
   +---------------------------+-------------------------+---------------------------------+--------------------------------+
   | |sr-long|                 | ``schema.registry.url`` | ``http://schema-registry:8081`` | ``http://localhost:8081``      |
   +---------------------------+-------------------------+---------------------------------+--------------------------------+
   | |zk|                      | ``zookeeper.connect``   | ``zookeeper:2181``              | ``localhost:2181``             |
   +---------------------------+-------------------------+---------------------------------+--------------------------------+

View messages in |ak| topics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :devx-examples:`docker-compose.yml|music/docker-compose.yml` file spins up a few containers, one of which is ``kafka-music-data-generator``, which is continuously generating input data for the music application by writing into two |ak| topics in Avro format.
This allows you to look at live, real-time data when testing the |ak| music application.

- ``play-events`` : stream of play events (“song X was played”)
- ``song-feed`` : stream of song metadata (“song X was written by artist Y”)

.. figure:: ../../../tutorials/examples/music/images/ksql-music-demo-source-data-explore.jpg
       :width: 600px

#. From your web browser, navigate to `Confluent Control Center <http://localhost:9021>`__.

#. Click on ``Topics`` and select any topic to view its messages.

   .. figure:: ../../../tutorials/examples/music/images/inspect_topic.png
          :width: 600px

#. You may also use the ksqlDB query editor in |c3| to view messages.  For example, to see the |ak| messages in ``play-events``, click on ``ksqlDB`` enter the following ksqlDB query into the editor:

   .. code-block:: bash
   
      PRINT "play-events";

#. Verify your output resembles:

   .. figure:: ../../../tutorials/examples/music/images/topic_ksql_play_events.png
          :width: 600px

#. Enter the following ksqlDB query into the editor to view the |ak| messages in ``song-feed``.

   .. code-block:: bash
   
      PRINT "song-feed" FROM BEGINNING;

#. You can also use command line tools to view messages in the |ak| topics. View the messages in the topic ``play-events``.

   .. codewithvars:: bash
  
       docker-compose exec schema-registry \
           kafka-avro-console-consumer \
               --bootstrap-server kafka:29092 \
               --topic play-events
   
#. Verify your output resembles:

   .. codewithvars:: bash

       {"song_id":11,"duration":60000}
       {"song_id":10,"duration":60000}
       {"song_id":12,"duration":60000}
       {"song_id":2,"duration":60000}
       {"song_id":1,"duration":60000}

#. View the messages in the topic ``song-feed``.

   .. codewithvars:: bash
  
       docker-compose exec schema-registry \
           kafka-avro-console-consumer \
               --bootstrap-server kafka:29092 \
               --topic song-feed \
               --from-beginning

#. Verify your output resembles:

   ::
  
     {"id":1,"album":"Fresh Fruit For Rotting Vegetables","artist":"Dead Kennedys","name":"Chemical Warfare","genre":"Punk"}
     {"id":2,"album":"We Are the League","artist":"Anti-Nowhere League","name":"Animal","genre":"Punk"}
     {"id":3,"album":"Live In A Dive","artist":"Subhumans","name":"All Gone Dead","genre":"Punk"}
     {"id":4,"album":"PSI","artist":"Wheres The Pope?","name":"Fear Of God","genre":"Punk"}


Validate the |kstreams| application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The |ak| music application has a REST API, run in the Docker container ``kafka-music-application``, that you can interactively query using ``curl``.

#. List all running application instances of the |ak| Music application.

   ::
  
     curl -sXGET http://localhost:7070/kafka-music/instances | jq .

#. Verify your output resembles:

   ::
  
       [
         {
           "host": "kafka-music-application",
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

     curl -sXGET http://localhost:7070/kafka-music/charts/top-five | jq .

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

Create the ksqlDB application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this section, create ksqlDB queries that are the equivalent to the |kstreams|.

.. figure:: ../../../tutorials/examples/music/images/ksql-music-demo-overview.jpg
    :width: 600px

You have two options to proceed:

- manually: step through the tutorial, creating each ksqlDB command one at a time
- automatically: submits all the :devx-examples:`ksqlDB commands|music/statements.sql` via the ksqlDB ``SCRIPT`` command:


Manually
++++++++

Prefix the names of the ksqlDB streams and tables with ``ksql_``.  This is not required but do it so that you can run these ksqlDB queries alongside the |kstreams| API version of this music demo and avoid naming conflicts.

#. Create a new stream called ``ksql_playevents`` from the ``play-events`` topic. From the ksqlDB application, select *Add a stream*.

   .. figure:: ../../../tutorials/examples/music/images/add_a_stream.png
          :width: 600px

#. Select the topic ``play-events``  and then fill out the fields as shown below.  Because |c3| integrates with |sr-long|, ksqlDB automatically detects the fields ``song_id`` and ``duration`` and their respective data types.

   .. figure:: ../../../tutorials/examples/music/images/ksql_playevents.png
          :width: 400px

#. Do some basic filtering on the newly created stream ``ksql_playevents``, e.g. to qualify songs that were played for at least 30 seconds.  From the ksqlDB query editor:

   .. code-block:: bash

      SELECT * FROM ksql_playevents WHERE DURATION > 30000 EMIT CHANGES;

#. The above query is not persistent. It stops if this screen is closed. To make the query persistent and stay running until explicitly terminated, prepend the previous query with ``CREATE STREAM ... AS``.  From the ksqlDB query editor:

   .. literalinclude:: ../../../tutorials/examples/music/statements.sql
      :lines: 9

#. Verify this persistent query shows up in the ``Running Queries`` tab.

#. The original Kafka topic ``song-feed`` has a key of type ``Long``, which maps to ksqlDB's ``BIGINT`` sql type, and the ID field stores a copy of the key. Create a ``TABLE`` from the original Kafka topic ``song-feed``:

   .. literalinclude:: ../../../tutorials/examples/music/statements.sql
      :lines: 12

#. View the contents of this table and confirm that the entries in this ksqlDB table have a ``ROWKEY`` that matches the String ID of the song.
 
   .. code-block:: bash

      SELECT * FROM ksql_song EMIT CHANGES limit 5;

#. ``DESCRIBE`` the table to see the fields associated with this topic and notice that the field ``ID`` is of type ``BIGINT``.
 
   .. figure:: ../../../tutorials/examples/music/images/describe_songfeed.png
       :width: 600px

#. At this point we have created a stream of filtered play events called ``ksql_playevents_min_duration`` and a table of song metadata called ``ksql_song``.  Enrich the stream of play events with song metadata using a Stream-Table ``JOIN``. This results in a new stream of play events enriched with descriptive song information like song title along with each play event.

   .. literalinclude:: ../../../tutorials/examples/music/statements.sql
      :lines: 16

#.  Notice the addition of a clause ``1 AS KEYCOL``. For every row, this creates a new field ``KEYCOL`` that has a value of 1. ``KEYCOL`` can be later used in other derived streams and tables to do aggregations on a global basis.

#. Now you can create a top music chart for all time to see which songs get played the most. Use the ``COUNT`` function on the stream ``ksql_songplays`` that we created above.

   .. literalinclude:: ../../../tutorials/examples/music/statements.sql
      :lines: 26

#. While the all-time greatest hits are cool, it would also be good to see stats for just the last 30 seconds. Create another query, adding in a ``WINDOW`` clause, which gives counts of play events for all songs, in 30-second intervals.

   .. literalinclude:: ../../../tutorials/examples/music/statements.sql
      :lines: 19

#. Congratulations, you built a streaming application that processes data in real-time!  The application enriched a stream of play events with song metadata and generated top counts. Any downstream systems can consume results from your ksqlDB queries for further processing.  If you were already familiar with SQL semantics, hopefully this tutorial wasn't too hard to follow.

   .. code-block:: bash

      SELECT * FROM ksql_songplaycounts30 EMIT CHANGES;

   .. figure:: ../../../tutorials/examples/music/images/counts_results.png
       :width: 600px

Automatically
+++++++++++++

#. View the :devx-examples:`ksqlDB statements.sql|music/statements.sql`.

   .. literalinclude:: ../../../tutorials/examples/music/statements.sql

#. Launch the ksqlDB CLI:

   .. code:: bash

      docker-compose exec ksqldb-cli ksql http://ksqldb-server:8088

#.  Run the script :devx-examples:`statements.sql|music/statements.sql` that executes the ksqlDB statements.

    .. code:: sql

        RUN SCRIPT '/tmp/statements.sql';

    The output shows either a blank message, or ``Executing statement``, similar to this:

    ::

         Message
        ---------
         Executing statement
        ---------

    After the ``RUN SCRIPT`` command completes, exit out of the ``ksqldb-cli`` with a ``CTRL+D`` command



Stop the music application
~~~~~~~~~~~~~~~~~~~~~~~~~~

#. When you are done, make sure to stop the demo.

   .. sourcecode:: bash

      docker-compose down


Troubleshooting
~~~~~~~~~~~~~~~

#. Verify the status of the Docker containers show ``Up`` state.

   .. code-block:: bash

        docker-compose ps

   Your output should resemble:

   .. code-block:: bash

                      Name                            Command                  State                            Ports                      
        -----------------------------------------------------------------------------------------------------------------------------------
        control-center                     /etc/confluent/docker/run        Up             0.0.0.0:9021->9021/tcp                          
        kafka                              /etc/confluent/docker/run        Up             0.0.0.0:29092->29092/tcp, 0.0.0.0:9092->9092/tcp
        kafka-music-application            bash -c echo Waiting for K ...   Up             0.0.0.0:7070->7070/tcp                          
        kafka-music-data-generator         bash -c echo Waiting for K ...   Up             7070/tcp                                        
        ksqldb-cli                         /bin/sh                          Up                                                             
        ksqldb-server                      /etc/confluent/docker/run        Up (healthy)   0.0.0.0:8088->8088/tcp                          
        schema-registry                    /etc/confluent/docker/run        Up             0.0.0.0:8081->8081/tcp                          
        zookeeper                          /etc/confluent/docker/run        Up             0.0.0.0:2181->2181/tcp, 2888/tcp, 3888/tcp     

#. |c3| displays messages from topics, streams, and tables as new messages arrive.  In this demo the data is sourced from an application running in a Docker container called ``kafka-music-data-generator``.  If you notice that |c3| is not displaying messages, you can try restarting this application.

   .. sourcecode:: bash

      docker-compose restart kafka-music-data-generator
