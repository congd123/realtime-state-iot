package com.ddss.data.riot.sources

import iot.trigger.Trigger
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.scalatest._
import org.scalatest.matchers.should.Matchers

class TriggerSourceTest extends FlatSpec with Matchers {

  "A Trigger Source" should "create a Source Function" in {
    val source = TriggerSource()

    source.isInstanceOf[SourceFunction[Trigger]] shouldBe true
  }

}
