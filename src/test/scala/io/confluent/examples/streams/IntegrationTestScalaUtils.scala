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
package io.confluent.examples.streams

import io.confluent.examples.streams.IntegrationTestUtils.NothingSerde
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer, Serdes => JSerdes}
import org.apache.kafka.streams.kstream.{Windowed, WindowedSerdes}
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}

/**
  * Makes the use of [[IntegrationTestUtils]] from Scala more convenient by providing implicit serializers and
  * deserializers. For example, it saves you from distinguishing between Scala's [[Int]] and Java's [[Integer]] when
  * reading from or writing to Kafka.
  */
object IntegrationTestScalaUtils {

  implicit def StringSerializer: Serializer[String] = JSerdes.String().serializer()

  implicit def LongSerializer: Serializer[Long] = JSerdes.Long().asInstanceOf[Serde[Long]].serializer()

  implicit def JavaLongSerializer: Serializer[java.lang.Long] = JSerdes.Long().serializer()

  implicit def ByteArraySerializer: Serializer[Array[Byte]] = JSerdes.ByteArray().serializer()

  implicit def BytesSerializer: Serializer[org.apache.kafka.common.utils.Bytes] = JSerdes.Bytes().serializer()

  implicit def FloatSerializer: Serializer[Float] = JSerdes.Float().asInstanceOf[Serde[Float]].serializer()

  implicit def JavaFloatSerializer: Serializer[java.lang.Float] = JSerdes.Float().serializer()

  implicit def DoubleSerializer: Serializer[Double] = JSerdes.Double().asInstanceOf[Serde[Double]].serializer()

  implicit def JavaDoubleSerializer: Serializer[java.lang.Double] = JSerdes.Double().serializer()

  implicit def IntegerSerializer: Serializer[Int] = JSerdes.Integer().asInstanceOf[Serde[Int]].serializer()

  implicit def JavaIntegerSerializer: Serializer[java.lang.Integer] = JSerdes.Integer().serializer()

  implicit def timeWindowedSerializer[T]: Serializer[Windowed[T]] =
    new WindowedSerdes.TimeWindowedSerde[T]().serializer()

  implicit def sessionWindowedSerializer[T]: Serializer[Windowed[T]] =
    new WindowedSerdes.SessionWindowedSerde[T]().serializer()

  implicit def StringDeserializer: Deserializer[String] = JSerdes.String().deserializer()

  implicit def LongDeserializer: Deserializer[Long] = JSerdes.Long().asInstanceOf[Serde[Long]].deserializer()

  implicit def JavaLongDeserializer: Deserializer[java.lang.Long] = JSerdes.Long().deserializer()

  implicit def ByteArrayDeserializer: Deserializer[Array[Byte]] = JSerdes.ByteArray().deserializer()

  implicit def BytesDeserializer: Deserializer[org.apache.kafka.common.utils.Bytes] = JSerdes.Bytes().deserializer()

  implicit def FloatDeserializer: Deserializer[Float] = JSerdes.Float().asInstanceOf[Serde[Float]].deserializer()

  implicit def JavaFloatDeserializer: Deserializer[java.lang.Float] = JSerdes.Float().deserializer()

  implicit def DoubleDeserializer: Deserializer[Double] = JSerdes.Double().asInstanceOf[Serde[Double]].deserializer()

  implicit def JavaDoubleDeserializer: Deserializer[java.lang.Double] = JSerdes.Double().deserializer()

  implicit def IntegerDeserializer: Deserializer[Int] = JSerdes.Integer().asInstanceOf[Serde[Int]].deserializer()

  implicit def JavaIntegerDeserializer: Deserializer[java.lang.Integer] = JSerdes.Integer().deserializer()

  implicit def timeWindowedDeserializer[T]: Deserializer[Windowed[T]] =
    new WindowedSerdes.TimeWindowedSerde[T]().deserializer()

  implicit def sessionWindowedDeserializer[T]: Deserializer[Windowed[T]] =
    new WindowedSerdes.SessionWindowedSerde[T]().deserializer()

  def produceValuesSynchronously[V](topic: String, values: Seq[V], driver: TopologyTestDriver)
                                   (implicit valueSerializer: Serializer[V]): Unit = {
    import collection.JavaConverters._
    driver.createInputTopic(topic,
                            new NothingSerde[Null],
                            valueSerializer)
      .pipeValueList(values.asJava)
  }

  def produceKeyValuesSynchronously[K, V](topic: String, records: Seq[KeyValue[K, V]], driver: TopologyTestDriver)
                                         (implicit keySer: Serializer[K], valueSer: Serializer[V]) {
    import collection.JavaConverters._
    driver.createInputTopic(topic, keySer, valueSer).pipeKeyValueList(records.asJava)
  }

  def drainStreamOutput[K, V](topic: String, driver: TopologyTestDriver)
                             (implicit keyDeser: Deserializer[K], valueDeser: Deserializer[V]): Seq[KeyValue[K, V]] = {
    import collection.JavaConverters._
    driver.createOutputTopic(topic, keyDeser, valueDeser).readKeyValuesToList().asScala
  }

  def drainTableOutput[K, V](topic: String, driver: TopologyTestDriver)
                            (implicit keyDeser: Deserializer[K], valueDeser: Deserializer[V]): Map[K, V] = {
    import collection.JavaConverters._
    driver.createOutputTopic(topic, keyDeser, valueDeser).readKeyValuesToMap().asScala.toMap
  }

}