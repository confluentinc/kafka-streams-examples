package io.confluent.examples.streams

import org.slf4j.{Logger, LoggerFactory}

trait LoggerTrait {
  protected val log: Logger = LoggerFactory.getLogger(getClass)
}
