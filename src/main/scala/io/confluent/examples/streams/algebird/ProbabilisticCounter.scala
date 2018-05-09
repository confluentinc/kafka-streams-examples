package io.confluent.examples.streams.algebird

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext

/**
  * Counts record values (in String format) probabilistically and then outputs the respective count estimate.
  */
class ProbabilisticCounter(val cmsStoreName: String)
  extends Transformer[Array[Byte], String, (String, Long)] {

  private var cmsState: CMSStore[String] = _
  private var processorContext: ProcessorContext = _

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    cmsState = this.processorContext.getStateStore(cmsStoreName).asInstanceOf[CMSStore[String]]
  }

  override def transform(key: Array[Byte], value: String): (String, Long) = {
    // Count the record value, think: "+ 1"
    cmsState.put(value, this.processorContext.timestamp())

    // In this example: emit the latest count estimate for the record value.  We could also do
    // something different, e.g. periodically output the latest heavy hitters via `punctuate`.
    (value, cmsState.get(value))
  }

  override def close(): Unit = {}
}