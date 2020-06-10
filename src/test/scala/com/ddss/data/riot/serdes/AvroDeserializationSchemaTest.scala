package com.ddss.data.riot.serdes

import java.io.ByteArrayOutputStream

import iot.state.{Device, State}
import iot.trigger.{Operator, Trigger, Device => DeviceTrigger}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.mockito.MockitoSugar
import org.scalatest._
import org.scalatest.matchers.should.Matchers

class AvroDeserializationSchemaTest extends FlatSpec with Matchers with MockitoSugar {

  "AvroDeserializationSchema" should "deserialize a state record" in {
    val record: ConsumerRecord[Array[Byte], Array[Byte]] = mock[ConsumerRecord[Array[Byte], Array[Byte]]]
    val schema = new Schema.Parser().parse(getClass.getResourceAsStream("/state.avsc"))
    val genericData = new GenericData.Record(schema) {
      put("id", "001")
      put("name", "device1")
      put("value", "1")
      put("timestamp", 10000)
      put("device", Device.PIR)
    }

    when(record.value()).thenReturn(genericRecordToBytes(schema, genericData))

    val deserializationSchema = new AvroDeserializationSchema(classOf[State])

    val state: State = deserializationSchema.deserialize(record)

    state.getId.toString shouldBe "001"
    state.getName.toString shouldBe "device1"
    state.getValue.toString shouldBe "1"
    state.getTimestamp shouldBe 10000
    state.getDevice shouldBe Device.PIR
  }

  it should "deserialize a trigger record" in {
    val record: ConsumerRecord[Array[Byte], Array[Byte]] = mock[ConsumerRecord[Array[Byte], Array[Byte]]]
    val schema = new Schema.Parser().parse(getClass.getResourceAsStream("/trigger.avsc"))

    val genericData = new GenericData.Record(schema) {
      put("id", "001")
      put("name", "trigger1")
      put("state", 1)
      put("timestamp", 10000)
      put("value", 2)
      put("operator", Operator.GREAT)
      put("device", DeviceTrigger.PIR)
    }

    when(record.value()).thenReturn(genericRecordToBytes(schema, genericData))

    val deserializationSchema = new AvroDeserializationSchema(classOf[Trigger])

    val trigger: Trigger = deserializationSchema.deserialize(record)

    trigger.getId.toString shouldBe "001"
    trigger.getName.toString shouldBe "trigger1"
    trigger.getState shouldBe 1
    trigger.getTimestamp shouldBe 10000
    trigger.getValue shouldBe 2
    trigger.getOperator shouldBe Operator.GREAT
    trigger.getDevice shouldBe DeviceTrigger.PIR
  }

  private def genericRecordToBytes(schema: Schema, genericRecord: GenericRecord): Array[Byte] = {
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericRecord, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }
}
