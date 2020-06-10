package com.ddss.data.riot.sources

import java.util.Properties

import com.examples.flink.streaming.serdes.AvroDeserializationSchema
import iot.trigger.Trigger
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

class TriggerSource(val topic: String, val properties: Properties) {

  def init(): SourceFunction[Trigger] = {
    new FlinkKafkaConsumer[Trigger](topic, new AvroDeserializationSchema[Trigger](classOf[Trigger]), properties)
  }
}

object TriggerSource {
  val topic = "devices.trigger"
  val properties = new Properties() {{
    put("bootstrap.servers", "kafka:19092")
    put("group.id", "consumer-triggers")
  }}

  def apply(): SourceFunction[Trigger] = {
    new TriggerSource(topic, properties).init()
  }
}