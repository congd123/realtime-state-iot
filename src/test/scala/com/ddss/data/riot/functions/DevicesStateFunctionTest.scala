package com.ddss.data.riot.functions

import java.util

import com.examples.flink.streaming.Descriptors
import iot.state.State
import iot.trigger.Trigger
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness
import org.scalatest._
import org.scalatest.matchers.should.Matchers

class DevicesStateFunctionTest extends FlatSpec with Matchers with BeforeAndAfter {

  var testHarness: KeyedBroadcastOperatorTestHarness[String, State, Trigger, State] = _

  before {
    val mapStateDescriptor: MapStateDescriptor[String, Trigger] = Descriptors.ruleTriggerDescriptor

    val list: util.List[MapStateDescriptor[_, _]] = new util.ArrayList[MapStateDescriptor[_, _]](1){{ add(mapStateDescriptor) }}

    val operator = new CoBroadcastWithKeyedOperator[String, State, Trigger, State](new DevicesStateFunction(), list)
    testHarness =
      new KeyedBroadcastOperatorTestHarness[String, State, Trigger, State](operator, (value: State) => value.getDevice.name().toUpperCase(), BasicTypeInfo.STRING_TYPE_INFO, 2, 2, 1)

    testHarness.open()
  }

  "DevicesStateFunction" should "trigger an alert for PIR when Operator GREAT" in {
    val trigger = new Trigger("id", "trigger1", 1, 10000, 10, iot.trigger.Operator.GREAT, iot.trigger.Device.PIR)
    testHarness.processBroadcastElement(trigger, 1L)

    val state = new State("001", "device1", "2", 10000, iot.state.Device.PIR)
    testHarness.processElement(state, 2L)

    testHarness.getOutput.element() shouldBe new StreamRecord(state, 2L)
  }

  it should "trigger an alert for PIR when Operator LESS" in {
    val trigger = new Trigger("id", "trigger1", 1, 10000, 1, iot.trigger.Operator.LESS, iot.trigger.Device.PIR)
    testHarness.processBroadcastElement(trigger, 1L)

    val state = new State("001", "device1", "2", 10000, iot.state.Device.PIR)
    testHarness.processElement(state, 2L)

    testHarness.getOutput.element() shouldBe new StreamRecord(state, 2L)
  }

  it should "trigger an alert for PIR when Operator EQUAL" in {
    val trigger = new Trigger("id", "trigger1", 1, 10000, 1, iot.trigger.Operator.EQUAL, iot.trigger.Device.PIR)
    testHarness.processBroadcastElement(trigger, 1L)

    val state = new State("001", "device1", "1", 10000, iot.state.Device.PIR)
    testHarness.processElement(state, 2L)

    testHarness.getOutput.element() shouldBe new StreamRecord(state, 2L)
  }

  it should "not trigger an alert" in {
    val trigger = new Trigger("id", "trigger1", 1, 10000, 2, iot.trigger.Operator.EQUAL, iot.trigger.Device.PIR)
    testHarness.processBroadcastElement(trigger, 1L)

    val state = new State("001", "device1", "1", 10000, iot.state.Device.PIR)
    testHarness.processElement(state, 2L)

    testHarness.getOutput.size() shouldBe 0
  }

}
