package io.confluent.examples.streams

import org.apache.kafka.streams.kstream.Reducer

import scala.language.implicitConversions

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object FunctionImplicits {

  implicit def BinaryFunctionToReducer[V](f: ((V, V) => V)): Reducer[V] = (l: V, r: V) => f(l, r)

}