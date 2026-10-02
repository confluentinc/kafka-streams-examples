# Ejemplos de Kafka Streams

<!-- hy-mt2-i18n:start -->
[English](./README.md) | [中文](./README_zh-CN.md) | [日本語](./README_ja.md) | **Español**
<!-- hy-mt2-i18n:end -->


> [!NOTA]
> Este repositorio ha sido reemplazado por [Confluent Tutorials for Apache Kafka](https://github.com/confluentinc/tutorials).
Todavía mantenemos su funcionamiento, pero ya no mejoramos los ejemplos existentes ni añadimos nuevos ejemplos.

Este proyecto contiene ejemplos de código que demuestran cómo implementar aplicaciones en tiempo real y microservicios basados en eventos utilizando la API Streams de [Apache Kafka](http://kafka.apache.org/), también conocida como Kafka Streams.

Para obtener más información, consulte la
[**documentación más reciente de Confluent sobre la API de Kafka Streams**](http://docs.confluent.io/current/streams/), en particular la
[**Guía para desarrolladores**](https://docs.confluent.io/platform/current/streams/developer-guide/index.html).


---
Índice

* [Ejemplos disponibles](#available-examples)
    * [Ejemplos: Aplicaciones ejecutables](#examples-apps)
    * [Ejemplos: Pruebas unitarias](#examples-unit-tests)
    * [Ejemplos: Pruebas de integración](#examples-integration-tests)
    * [Ejemplo con Docker: Aplicación de demostración musical con Kafka](#examples-docker)
    * [Ejemplos: Plataforma de transmisión de eventos](#examples-event-streaming-platform)
* [Requisitos](#requirements)
    * [Apache Kafka](#requirements-kafka)
    * [Confluent Platform](#requirements-confluent-platform)
    * [Uso de IntelliJ o Eclipse](#requirements-ide)
    * [Java](#requirements-java)
    * [Scala](#requirements-scala)
* [Empaquetado y ejecución de los ejemplos](#packaging-and-running)
* [Desarrollo](#development)
* [Matriz de compatibilidad de versiones](#version-compatibility)
* [Dónde encontrar ayuda](#help)

# Ejemplos disponibles


<a name="available-examples"/>

# Ejemplos disponibles

Este repositorio cuenta con varias ramas que le ayudarán a encontrar los ejemplos de código adecuados para la versión de Apache Kafka y/o Confluent Platform que está utilizando. Consulte la [Matriz de compatibilidad de versiones](#version-compatibility) a continuación para más detalles.

Existen tres tipos de ejemplos:

* **Ejemplos en [src/main/](src/main/)**: Estos ejemplos son cortos y concisos. Además, puede probarlos de forma interactiva, por ejemplo, contra un clúster local de Kafka. Si desea ejecutar realmente estos ejemplos, primero debe instalar y ejecutar Apache Kafka y las herramientas relacionadas, lo cual describimos en la sección [Empaquetado y ejecución de ejemplos](#packaging-and-running). Cada ejemplo también indica sus requisitos exactos e instrucciones en la parte superior.  
* **Ejemplos en [src/test/](src/test/)**: Estos ejemplos tienen como objetivo probar las aplicaciones ubicadas en [src/main/](src/main/). Las pruebas unitarias con TopologyTestDriver evalúan la lógica de flujo sin depender de sistemas externos. Las pruebas de integración utilizan clústeres de Kafka integrados, les envían datos de entrada (mediante el cliente productor estándar de Kafka), procesan los datos con Kafka Streams y, finalmente, leen y verifican los resultados de salida (usando el cliente consumidor estándar de Kafka). Estos ejemplos también constituyen un buen punto de partida para aprender cómo implementar sus propias pruebas de integración de extremo a extremo.  
* **Ejemplos Docker listos para ejecutarse**: Estos ejemplos ya están compilados y empaquetados en contenedores.


<a name="examples-apps"/>

## Ejemplos: Aplicaciones ejecutables

Se pueden encontrar ejemplos adicionales en [src/main/](src/main/java/io/confluent/examples/streams/).

| Nombre de la aplicación     | Conceptos utilizados                                      | Java 8+ | Java 7+ | Scala |
| --------------------------- | -------------------------------------------------------- | ------- | ------- | ----- |
| WordCount                   | DSL, agregación, con estado                                | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java) | | [Ejemplo en Scala](src/main/scala/io/confluent/examples/streams/WordCountScalaExample.scala) |
| MapFunction                 | DSL, transformaciones sin estado, `map()`                  | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java) | | [Ejemplo en Scala](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala) |
| SessionWindows              | Sesionización de eventos de usuario, análisis del comportamiento del usuario | | [Ejemplo en Java 7+](src/main/java/io/confluent/examples/streams/SessionWindowsExample.java)
| GlobalKTable                | `join()` entre `KStream` y `GlobalKTable`            | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/GlobalKTablesExample.java) | | |
| GlobalStore                 | "join" entre `KStream` y `GlobalStore`               | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/GlobalStoresExample.java) | | |
| PageViewRegion              | `join()` entre `KStream` y `KTable`                  | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java) | [Ejemplo en Java 7+](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) | |
| PageViewRegionGenericAvro   | Trabajo con datos en formato Generic Avro                 | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java) | [Ejemplo en Java 7+](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) | |
| WikipediaFeedSpecificAvro   | Trabajo con datos en formato Specific Avro                | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroLambdaExample.java) | [Ejemplo en Java 7+](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroExample.java) | |
| SecureKafkaStreams          | Seguridad, cifrado, autenticación de clientes                | | [Ejemplo en Java 7+](src/main/java/io/confluent/examples/streams/SecureKafkaStreamsExample.java) | |
| Sum                         | DSL, transformaciones con estado, `reduce()`                | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/SumLambdaExample.java) | | |
| WordCountInteractiveQueries | Consultas interactivas, REST, RPC                           | [Ejemplo en Java 8+](src/main/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExample.java) | | |
| KafkaMusic                  | Consultas interactivas, almacenes de estado, API REST              | [Ejemplo para Java 8+](src/main/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExample.java) | | |
| ApplicationReset            | Herramienta de reinicio de aplicaciones `kafka-streams-application-reset` | [Ejemplo para Java 8+](src/main/java/io/confluent/examples/streams/ApplicationResetExample.java) | | |
| Microservice                | Ecosistema de microservicios, almacenes de estado, enrutamiento dinámico, uniones, filtrado, ramificación, operaciones con estado | [Ejemplo para Java 8+](src/main/java/io/confluent/examples/streams/microservices) | | |


<a name="examples-unit-tests"/>

## Ejemplos: Pruebas unitarias

El procesamiento de flujos de Kafka Streams puede someterse a **pruebas unitarias** mediante el `TopologyTestDriver` del artefacto `org.apache.kafka:kafka-streams-test-utils`. Este controlador de pruebas permite escribir datos de entrada de ejemplo en la topología de procesamiento y validar su salida.

Consulte la documentación en [Testing Streams Code](https://docs.confluent.io/current/streams/developer-guide/test-streams.html).


<a name="examples-integration-tests"/>

## Ejemplos: Pruebas de integración

También ofrecemos varias **pruebas de integración**, que demuestran pipelines de datos de extremo a extremo. En ellas, se crean clústeres Kafka incrustados y el [Confluent Schema Registry](https://github.com/confluentinc/schema-registry); se les envía datos de entrada (utilizando el cliente productor estándar de Kafka), se procesan los datos con Kafka Streams, y finalmente se leen y verifican los resultados de salida (usando el cliente consumidor estándar de Kafka).

Se pueden encontrar ejemplos adicionales en [src/test/](src/test/java/io/confluent/examples/streams/).

> Consejo: Ejecute `mvn test` para iniciar las pruebas.

| Nombre de la prueba de integración       | Conceptos utilizados                          | Java 8+ | Java 7+ | Scala |
| ----------------------------------- | ------------------------------------------- | ------- | ------- | ----- |
| WordCount                           | DSL, agregación, estado                    | [Ejemplo en Java 8+](src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java) | | [Ejemplo en Scala](src/test/scala/io/confluent/examples/streams/WordCountScalaIntegrationTest.scala) |
| WordCountInteractiveQueries         | Consultas interactivas, REST, RPC          | | [Ejemplo en Java 7+](src/test/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesExampleTest.java) | |
| Aggregate                           | DSL, `groupBy()`, `aggregate()`             | [Ejemplo en Java 8+](src/test/java/io/confluent/examples/streams/AggregateTest.java) | | [Ejemplo en Scala](src/test/scala/io/confluent/examples/streams/AggregateScalaTest.scala) |
| CustomStreamTableJoin               | DSL, API de procesador, Transformadores   | [Ejemplo en Java 8+](src/test/java/io/confluent/examples/streams/CustomStreamTableJoinIntegrationTest.java) | | |
| EventDeduplication                  | DSL, API de procesador, Transformadores   | [Ejemplo en Java 8+](src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java) | | |
| GlobalKTable                        | DSL, estado global                           | | [Ejemplo en Java 7+](src/test/java/io/confluent/examples/streams/GlobalKTablesExampleTest.java) | |
| GlobalStore                         | DSL, estado global, Transformadores       | | [Ejemplo en Java 7+](src/test/java/io/confluent/examples/streams/GlobalStoresExampleTest.java) | |
| HandlingCorruptedInputRecords       | DSL, `flatMap()`                            | [Ejemplo en Java 8+](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java) | | |
| KafkaMusic (Consultas interactivas)    | Consultas interactivas, Almacenes de estado, API REST | | [Ejemplo en Java 7+](src/test/java/io/confluent/examples/streams/interactivequeries/kafkamusic/KafkaMusicExampleTest.java) | |
| MapFunction                         | DSL, transformaciones sin estado, `map()`     | [Ejemplo en Java 8+](src/test/java/io/confluent/examples/streams/MapFunctionLambdaIntegrationTest.java) | | |
| MixAndMatch DSL + API de procesador     | Integración de DSL y API de procesador      | [Ejemplo en Java 8+](src/test/java/io/confluent/examples/streams/MixAndMatchLambdaIntegrationTest.java) | | |
| PassThrough                         | DSL, `stream()`, `to()`                     | | [Ejemplo en Java 7+](src/test/java/io/confluent/examples/streams/PassThroughIntegrationTest.java) | |
| PoisonPill                          | DSL, `flatMap()`                            | [Ejemplo para Java 8+](src/test/java/io/confluent/examples/streams/HandlingCorruptedInputRecordsIntegrationTest.java) | | |
| ProbabilisticCounting\*\*\*         | DSL, API de procesadores, almacenes de estado personalizados | | | [Ejemplo en Scala](src/test/scala/io/confluent/examples/streams/ProbabilisticCountingScalaIntegrationTest.scala) |
| Reduce (Concatenate)                | DSL, `groupByKey()`, `reduce()`             | [Ejemplo para Java 8+](src/test/java/io/confluent/examples/streams/ReduceTest.java) | | [Ejemplo en Scala](src/test/scala/io/confluent/examples/streams/ReduceScalaTest.scala) |
| SessionWindows                      | DSL, agregación por ventanas, sesionización   | | [Ejemplo para Java 7+](src/test/java/io/confluent/examples/streams/SessionWindowsExampleTest.java) | |
| StatesStoresDSL                     | DSL, API de procesadores, transformadores            | [Ejemplo para Java 8+](src/test/java/io/confluent/examples/streams/StateStoresInTheDSLIntegrationTest.java) | | |
| StreamToStreamJoin                  | DSL, `join()` entre KStream y KStream   | | [Ejemplo para Java 7+](src/test/java/io/confluent/examples/streams/StreamToStreamJoinIntegrationTest.java) | |
| StreamToTableJoin                   | DSL, `join()` entre KStream y KTable    | | [Ejemplo para Java 7+](src/test/java/io/confluent/examples/streams/StreamToTableJoinIntegrationTest.java) | [Ejemplo en Scala](src/test/scala/io/confluent/examples/streams/StreamToTableJoinScalaIntegrationTest.scala) |
| Sum                                 | DSL, agregación, estado, `reduce()`      | [Ejemplo para Java 8+](src/test/java/io/confluent/examples/streams/SumLambdaIntegrationTest.java) | | |
| TableToTableJoin                    | DSL, `join()` entre KTable y KTable     | | [Ejemplo para Java 7+](src/test/java/io/confluent/examples/streams/TableToTableJoinIntegrationTest.java) | |
| UserCountsPerRegion                 | DSL, agregación, estado, `count()`       | [Ejemplo para Java 8+](src/test/java/io/confluent/examples/streams/UserCountsPerRegionLambdaIntegrationTest.java) | | |
| ValidateStateWithInteractiveQueries | Consultas interactivas para validar el estado    | | [Ejemplo para Java 8+](src/test/java/io/confluent/examples/streams/ValidateStateWithInteractiveQueriesLambdaIntegrationTest.java) | | |
| GenericAvro                         | Trabajo con datos en formato Generic Avro    | | [Ejemplo para Java 7+](src/test/java/io/confluent/examples/streams/GenericAvroIntegrationTest.java) |  [Ejemplo en Scala](src/test/scala/io/confluent/examples/streams/GenericAvroScalaIntegrationTest.scala) |
| SpecificAvro                        | Trabajo con datos en formato Specific Avro   | | [Ejemplo para Java 7+](src/test/java/io/confluent/examples/streams/SpecificAvroIntegrationTest.java) | [Ejemplo para Scala](src/test/scala/io/confluent/examples/streams/SpecificAvroScalaIntegrationTest.scala) |

***demuestra cómo contar de forma probabilística los elementos en un flujo de entrada mediante la implementación de un almacén de estado personalizado ([CMSStore](src/main/scala/io/confluent/examples/streams/algebird/CMSStore.scala)) respaldado por una estructura de datos [Count-Min Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) (con la implementación CMS de [Twitter Algebird](https://github.com/twitter/algebird)).***


<a name="examples-docker"/>

# Ejemplo en Docker: Aplicación de demostración Kafka Music

Este ejemplo en contenedor inicia:

* La aplicación de ejemplo Kafka Music de Confluent para la API Kafka Streams, que utiliza las
  [Consultas interactivas](http://docs.confluent.io/current/streams/developer-guide.html)
* un clúster de Apache Kafka de un solo nodo con un conjunto ZooKeeper de un solo nodo
* una instancia de [Confluent Schema Registry](https://github.com/confluentinc/schema-registry)

La aplicación Kafka Music muestra cómo crear una sencilla aplicación de listas musicales que calcula de forma continua, en tiempo real, las listas más recientes, como los 5 mejores canciones por género musical. Expose sus resultados de procesamiento más recientes —las listas más recientes— a través de la función de [Consultas Interactivas](http://docs.confluent.io/current/streams/developer-guide.html#interactive-queries) de Kafka, mediante una API REST. Los datos de entrada de la aplicación están en formato Avro, por lo que se utiliza Confluent Schema Registry; dichos datos provienen de dos fuentes: un flujo de eventos de reproducción (“se reprodujo la canción X”) y un flujo de metadatos de canciones (“la canción X fue compuesta por el artista Y”).

Puede encontrar documentación detallada en  
https://docs.confluent.io/current/streams/kafka-streams-examples/docs/index.html.


#a Plataforma de Flujo de Eventos
Para ejemplos adicionales que muestran aplicaciones de Kafka Streams dentro de una plataforma de flujo de eventos, consulte el [repositorio de ejemplos en GitHub](https://github.com/confluentinc/examples).

# Ejemplos: Plataforma de transmisión de eventos

Para ver ejemplos adicionales que muestran aplicaciones de Kafka Streams dentro de una plataforma de streaming de eventos, consulte el [repositorio de GitHub de ejemplos](https://github.com/confluentinc/examples).


# Requisitos</a>

# Requisitos

<a name="requirements-kafka"/>

## Apache Kafka

El código de este repositorio requiere Apache Kafka 0.10+ ya que a partir de esta versión Kafka incluye su biblioteca [Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams). Consulte la [Matriz de Compatibilidad de Versiones](#version-compatibility) para obtener más detalles, ya que las diferentes ramas de este repositorio pueden tener requisitos distintos en cuanto a Kafka.

> **Para la rama `master`:** Para compilar una versión de desarrollo, por lo general se necesita la versión más reciente `trunk` de Apache Kafka
> (consulte `kafka.version` en [pom.xml](pom.xml) para más detalles). Las siguientes instrucciones compilarán e instalarán localmente
> la versión más reciente `trunk` de Kafka:
>
> ```shell
> $ git clone git@github.com:apache/kafka.git
> $ cd kafka
> $ git checkout trunk
>
> # Ahora compile e instale Kafka localmente
> $./gradlew clean &&./gradlewAll install
> ```


<a name="requirements-confluent-platform"/>

## Confluent Platform

El código de este repositorio requiere [Confluent Schema Registry](https://github.com/confluentinc/schema-registry).  
Consulte la [Matriz de Compatibilidad de Versiones](#version-compatibility) para obtener más detalles, ya que las diferentes ramas de este repositorio tienen requisitos distintos en cuanto a Confluent Platform.

* [Guía de inicio rápido de Confluent Platform](http://docs.confluent.io/current/quickstart.html) (cómo descargar e instalar)
* [Documentación de Confluent Platform](http://docs.confluent.io/current/)

> **Para la rama `master`:** Para compilar una versión de desarrollo, generalmente se necesita la última versión `master` de Schema Registry de Confluent Platform
> (véase `confluent.version` en [pom.xml](pom.xml), el cual es establecido por el proyecto upstream
> [Confluent Common](https://github.com/confluentinc/common)).
> Las siguientes instrucciones compilarán e instalarán localmente la última versión `master` de Schema Registry, lo que incluye
> la compilación de sus dependencias como [Confluent Common](https://github.com/confluentinc/common) y
> [Confluent Rest Utils](https://github.com/confluentinc/rest-utils).
> Lea la [README de Schema Registry](https://github.com/confluentinc/schema-registry) para más detalles.
>
> ```shell
> $ git clone https://github.com/confluentinc/common.git
> $ cd common
> $ git checkout master
>
> # Compilar e instalar common localmente
> $ mvn -DskipTests=true clean install
>
> $ git clone https://github.com/confluentinc/rest-utils.git
> $ cd rest-utils
> $ git checkout master
>
> # Compilar e instalar rest-utils localmente
> $ mvn -DskipTests=true clean install
>
> $ git clone https://github.com/confluentinc/schema-registry.git
> $ cd schema-registry
> $ git checkout master
>
> # Ahora compilar e instalar schema-registry localmente
> $ mvn -DskipTests=true clean install
> ```

Además, cada ejemplo indica sus requisitos exactos en la parte superior.


<a name="requirements-ide"/>

## Uso de IntelliJ o Eclipse

Si está utilizando un IDE e importa el proyecto, es posible que aparezca un error de “missing import / class not found”.  
Algunas clases Avro se generan a partir de archivos de esquema y los IDEs a veces no generan estas clases automáticamente.  
Para resolver este error, ejecute manualmente:

```shell
$ mvn -DskipTests=true compile
```

Si está utilizando Eclipse, también puede hacer clic con el botón derecho en el archivo `pom.xml` y seleccionar _Ejecutar como > Maven generate-sources_.


<a name="requirements-java"/>

## Java 17+

Usuarios de IntelliJ IDEA:

* Abra _Archivo > Estructura del proyecto_.
* Seleccione “Proyecto” en el lado izquierdo.
    * Establezca “SDK del proyecto” en Java 17.
    * Establezca “Nivel de lenguaje del proyecto” en “17 - Tipos sellados, semántica de punto flotante siempre estricta”.


<a name="requirements-scala"/>

## Scala

El Scala solo es necesario para los ejemplos en Scala de este repositorio. Si eres desarrollador en Java, puedes ignorar esta sección sin problemas.

Si desea probar los ejemplos en Scala de este repositorio, necesita una versión de Scala que soporte Java 17.

<a name="packaging-and-running"/>

# Empaquetado y ejecución de los ejemplos de aplicación

Las instrucciones de esta sección son necesarias únicamente si desea probar de forma interactiva los [ejemplos de aplicación](#examples-apps) ubicados en [src/main/](src/main/).

> **Consejo:** Si solo desea ejecutar las pruebas de integración (`mvn test`), no es necesario empaquetar ni instalar nada; basta con ejecutar `mvn test`. Estas pruebas inician clústeres Kafka integrados.

El primer paso es instalar y ejecutar un clúster de Kafka, el cual debe constar al menos en un broker de Kafka además de al menos una instancia de ZooKeeper. Algunos ejemplos también pueden requerir una instancia en ejecución del registro de esquemas de Confluent. La guía [Confluent Platform Quickstart](http://docs.confluent.io/current/quickstart.html) ofrece todos los detalles.

En resumen:

```shell
# Asegúrese de haber descargado e instalado Confluent Platform siguiendo las instrucciones del Guía Rápida anterior.

# Generar un UUID para el clúster
$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# Formatear las carpetas de registros
$ bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/kraft/reconfig-server.properties

# Iniciar el broker de Kafka
$./bin/kafka-server-start./etc/kafka/server.properties

# En una terminal separada, iniciar Confluent Schema Registry
$./bin/schema-registry-start./etc/schema-registry/schema-registry.properties

# Nuevamente, consulte la Guía Rápida de Confluent Platform para obtener detalles como
# cómo descargar Confluent Platform, cómo detener los tres servicios anteriores, etc.
```

El siguiente paso es crear un archivo jar independiente (“fat jar”) de las [ejemplos de aplicación](#examples-apps):

```shell
# Crear un jar independiente (“fat jar”)
$ mvn clean package

# >>> Crea target/kafka-streams-examples-8.4.0-0-standalone.jar
```

> Consejo: Si es necesario, puede desactivar el conjunto de pruebas durante el empaquetado, por ejemplo para acelerar el proceso o reducir el uso de memoria de la JVM:
>
> ```shell
> $ mvn -DskipTests=true clean package
> ```

Ahora puede ejecutar los ejemplos de aplicación de la siguiente manera:

# Ejecutar una aplicación de ejemplo desde el jar independiente. En este caso: `WordCountLambdaExample`
$ java -cp target/kafka-streams-examples-8.4.0-0-standalone.jar \
  io.confluent.examples.streams.WordCountLambdaExample

La aplicación intentará leer desde el tema de entrada especificado (en el ejemplo anterior es ``streams-plaintext-input``), ejecutar la lógica de procesamiento y luego intentar escribir de nuevo en el tema de salida especificado (en el ejemplo anterior es ``streams-wordcount-output``). Para observar el flujo de salida esperado, será necesario iniciar un productor de consola para enviar mensajes al tema de entrada y un consumidor de consola para leer continuamente del tema de salida. Se pueden encontrar más detalles sobre cómo ejecutar los ejemplos en las [documentaciones de Java](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java#L31) de cada código de ejemplo.

Si desea activar log4j2 al ejecutar su aplicación de ejemplo, puede editar el archivo
[log4j2.yaml](src/main/resources/log4j2.yaml) y luego ejecutarlo de la siguiente manera:

```shell
# Ejecutar una aplicación de ejemplo desde el archivo jar independiente. En este caso: `WordCountLambdaExample`
$ java -cp target/kafka-streams-examples-8.4.0-0-standalone.jar \
  -Dlog4j2.configurationFile=src/main/resources/log4j2.yaml \
  io.confluent.examples.streams.WordCountLambdaExample
```

Tenga en cuenta que la máquina en la que ejecute la comando anterior debe tener acceso a los clústeres Kafka/ZooKeeper que configuró en los ejemplos de código. Por defecto, los ejemplos de código suponen que el clúster Kafka es accesible a través de `localhost:9092` (también conocido como el parámetro ``bootstrap.servers`` de Kafka) y el conjunto ZooKeeper a través de `localhost:2181`. Puede sobrescribir el parámetro predeterminado ``bootstrap.servers`` mediante un argumento de línea de comandos.


#a Desarrollo

# Desarrollo

Este proyecto utiliza el ciclo de vida estándar de Maven y comandos como:

```shell
$ mvn compile # Esto también genera clases Java a partir de los esquemas Avro
$ mvn test    # Ejecuta pruebas unitarias e de integración
$ mvn package # Empaqueta los ejemplos de aplicación en un archivo jar independiente
```


<a name="version-compatibility"/>

# Matriz de compatibilidad de versiones

| Rama (este repositorio)                      | Confluent Platform | Apache Kafka      |
| ----------------------------------------|--------------------|-------------------|
| [master](../../../tree/master/)\*       | 8.0.0-SNAPSHOT     | 4.0.0-SNAPSHOT    |
| [7.9.x](../../../tree/7.9.x/)           | 7.9.0-SNAPSHOT     | 3.9.0             |
| [7.8.0-post](../../../tree/7.8.0-post/) | 7.8.0              | 3.8.0             |
|...                                     |                    |                   |
| [7.1.0-post](../../../tree/7.1.0-post/) | 7.1.0              | 3.1.0             |

Las versiones anteriores a 7.1.0 [ya no están soportadas](https://docs.confluent.io/platform/current/installation/versions-interoperability.html).

\*Debe compilar manualmente la versión `4.0` de Apache Kafka y la versión `8.0.0` de Confluent Platform. Consulte las instrucciones anteriores.

La rama `master` de este repositorio representa el desarrollo activo, y es posible que sea necesario realizar pasos adicionales por su parte para poder compilarla. Consulte este README así como el archivo [pom.xml](pom.xml) en busca de dicha información.


<a name="help"/>

# ¿Dónde encontrar ayuda?

* ¿Busca documentación sobre la API Streams de Apache Kafka?
    * Le recomendamos leer el [capítulo sobre Kafka Streams](https://docs.confluent.io/current/streams/) en la
      [documentación de Confluent Platform](https://docs.confluent.io/current/).
    * Vea nuestro video
      [Rethinking Stream Processing with Apache Kafka](https://www.youtube.com/watch?v=ACwnrnVJXuE)
* ¿Tiene problemas al usar las demostraciones y ejemplos de este proyecto?
    * Primero, debería consultar nuestra [wiki de Preguntas Frecuentes](https://github.com/confluentinc/kafka-streams-examples/wiki/FAQ) en busca de una respuesta.
    * Si la FAQ no le ayuda, [cree un nuevo problema en GitHub](https://github.com/confluentinc/kafka-streams-examples/issues).
* ¿Desea hacer una pregunta, informar sobre un error en Kafka o su API Kafka Streams, o solicitar una nueva función para Kafka?
    * Para preguntas generales sobre Apache Kafka y Confluent Platform, visite la
      [lista de correo de Confluent](https://groups.google.com/forum/?pli=1#!forum/confluent-platform)
      o las [listas de correo de Apache Kafka](http://kafka.apache.org/contact).

# Licencia

El uso de esta imagen está sujeto a los términos de licencia del software que contiene. Consulte la documentación sobre imágenes Docker de Confluent [referencia](https://docs.confluent.io/platform/current/installation/docker/image-reference.html) para obtener más información. El software necesario para ampliar y crear imágenes Docker personalizadas está disponible bajo la Licencia Apache 2.0.
