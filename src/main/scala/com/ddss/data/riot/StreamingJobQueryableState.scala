/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ddss.data.riot

import java.util.Properties

import com.examples.flink.streaming.functions.DevicesStateFunction
import com.examples.flink.streaming.serdes.AvroSerializationSchema
import com.examples.flink.streaming.sources.{StateSource, TriggerSource}
import iot.state.State
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object StreamingJobQueryableState {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)

    val producerProperties = new Properties() {{
      put("bootstrap.servers", "kafka:19092")
    }}

    val producer = new FlinkKafkaProducer[State]("devices.state", new AvroSerializationSchema[State](classOf[State], "devices.alert"), producerProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

    val ruleTriggerDescriptor = Descriptors.ruleTriggerDescriptor

    val dataStream = env
      .addSource(StateSource())
      .keyBy((record: State) => record.getDevice.name().toUpperCase())
      .connect(env.addSource(TriggerSource()).broadcast(ruleTriggerDescriptor))
      .process(new DevicesStateFunction())
      .addSink(producer)

    env.execute("iot devices state");
  }
}