package com.ddss.data.riot.functions

import com.examples.flink.streaming.Descriptors
import com.examples.flink.streaming.serdes.AvroJsonStringSerialization
import com.typesafe.scalalogging.LazyLogging
import iot.state.State
import iot.trigger.{Operator, Trigger}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

/**
 * Process Function to process each device state change and/or triggers created to generate alerts
 */
class DevicesStateFunction extends KeyedBroadcastProcessFunction[String, State, Trigger, State] with LazyLogging{

  var windowDevicesState: MapState[String, String] = _

  override def processElement(state: State, ctx: KeyedBroadcastProcessFunction[String, State, Trigger, State]#ReadOnlyContext, out: Collector[State]): Unit = {
    logger.debug("Processing device state. state={}", state)

    val rule = ctx.getBroadcastState(Descriptors.ruleTriggerDescriptor).get(state.getDevice.name().toUpperCase)

    if (rule != null) {
      rule.getOperator match {
        case Operator.EQUAL =>
          if (rule.getValue == Integer.valueOf(state.getValue.toString)) {
            logger.debug("Rule EQUAL {} triggered on device {}", rule.getName.toString, state)

            windowDevicesState.put(state.getId.toString, AvroJsonStringSerialization.serialize(state))
            out.collect(state)
          }
        case Operator.GREAT =>
          if (rule.getValue > Integer.valueOf(state.getValue.toString)) {
            logger.debug("Rule GREAT {} triggered on device {}", rule.getName.toString, state)

            windowDevicesState.put(state.getId.toString, AvroJsonStringSerialization.serialize(state))
            out.collect(state)
          }
        case Operator.LESS =>
          if (rule.getValue < Integer.valueOf(state.getValue.toString)) {
            logger.debug("Rule LESS {} triggered on device {}", rule.getName.toString, state)

            windowDevicesState.put(state.getId.toString, AvroJsonStringSerialization.serialize(state))
            out.collect(state)
          }
        case _ =>
          windowDevicesState.put(state.getId.toString, AvroJsonStringSerialization.serialize(state))
      }
    }
  }

  override def processBroadcastElement(value: Trigger, ctx: KeyedBroadcastProcessFunction[String, State, Trigger, State]#Context, out: Collector[State]): Unit = {
    ctx.getBroadcastState(Descriptors.ruleTriggerDescriptor).put(value.getDevice.name().toUpperCase(), value)
  }

  override def open(parameters: Configuration): Unit = {
    val mapStateDescriptor = new MapStateDescriptor[String, String](
      "windowDevicesState",
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    mapStateDescriptor.setQueryable("devices-state")

    windowDevicesState = getRuntimeContext.getMapState[String, String](mapStateDescriptor)
  }
}
