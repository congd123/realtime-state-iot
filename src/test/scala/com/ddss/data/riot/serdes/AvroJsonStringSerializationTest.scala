package com.ddss.data.riot.serdes

import iot.state.Device
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest._
import org.scalatest.matchers.should.Matchers

class AvroJsonStringSerializationTest extends FlatSpec with Matchers {

  "AvroJsonStringSerialization" should "convert State record into Json" in {
    val schema = new Schema.Parser().parse(getClass.getResourceAsStream("/state.avsc"))
    val genericData = new GenericData.Record(schema) {
      put("id", "001")
      put("name", "device1")
      put("value", "1")
      put("timestamp", 10000)
      put("device", Device.PIR)
    }

    val json = AvroJsonStringSerialization.serialize(genericData)
    json shouldBe "{\"id\":\"001\",\"name\":\"device1\",\"value\":\"1\",\"timestamp\":10000,\"device\":\"PIR\"}"
  }

  it should "throw NPE when missing a required field" in {
    intercept[NullPointerException] {
      val schema = new Schema.Parser().parse(getClass.getResourceAsStream("/state.avsc"))
      val genericData = new GenericData.Record(schema) {
        put("id", "001")
        put("timestamp", 10000)
        put("device", Device.PIR)
      }

      AvroJsonStringSerialization.serialize(genericData)
    }
  }
}
