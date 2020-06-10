package com.ddss.data.riot.serdes

import java.io.ByteArrayOutputStream

import iot.state.State
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.avro.specific.SpecificDatumWriter

/**
 * Serialization of [[State]] to a Json String
 */
object AvroJsonStringSerialization {

  lazy val schema: Schema = new Schema.Parser().parse(getClass.getResourceAsStream("/state.avsc"))

  def serialize(element: GenericRecord): String = {
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: JsonEncoder = EncoderFactory.get().jsonEncoder(schema, out, false)
    writer.write(element, encoder)
    encoder.flush()
    out.close()
    out.toString("UTF-8")
  }
}
