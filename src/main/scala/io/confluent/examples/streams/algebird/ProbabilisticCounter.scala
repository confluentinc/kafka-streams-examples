package io.confluent.examples.streams.algebird

import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}

/**
  * Counts record values (in String format) probabilistically and then outputs the respective count estimate.
  */
class ProbabilisticCounter(val cmsStoreName: String)
  extends Processor[String, String, String, Long] {

  private var cmsState: CMSStore[String] = _
  private var processorContext: ProcessorContext[String, Long] = _

  override def init(processorContext: ProcessorContext[String, Long]): Unit = {
    this.processorContext = processorContext
    cmsState = this.processorContext.getStateStore[CMSStore[String]](cmsStoreName)
  }

  override def process(record: Record[String, String]): Unit = {
    // Count the record value, think: "+ 1"
    cmsState.put(record.value(), record.timestamp())

    // In this example: emit the latest count estimate for the record value.  We could also do
    // something different, e.g. periodically output the latest heavy hitters via `punctuate`.
    processorContext.forward(new Record(record.value(), cmsState.get(record.value()), record.timestamp(), record.headers()))
  }
}