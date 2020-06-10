package com.ddss.data.riot.sources

import java.util.Properties

import com.examples.flink.streaming.serdes.AvroDeserializationSchema
import iot.state.State
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

class StateSource(val topic: String, val properties: Properties) {

  def init(): SourceFunction[State] = {
    new FlinkKafkaConsumer[State]("devices.state", new AvroDeserializationSchema[State](classOf[State]), properties)
  }
}

object StateSource {
  val topic = "devices.state"
  val properties = new Properties() {{
    put("bootstrap.servers", "kafka:19092")
    put("group.id", "consumer-states")
  }}

  def apply(): SourceFunction[State] = {
    new StateSource(topic, properties).init()
  }
}

