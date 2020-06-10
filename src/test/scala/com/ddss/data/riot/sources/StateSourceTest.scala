package com.ddss.data.riot.sources

import iot.state.State
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.scalatest._
import org.scalatest.matchers.should.Matchers

class StateSourceTest extends FlatSpec with Matchers {

  "A State Source" should "create a Source Function" in {
    val source = StateSource()

    source.isInstanceOf[SourceFunction[State]] shouldBe true
  }

}
