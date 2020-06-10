package com.ddss.data.riot.serdes

import java.io.ByteArrayOutputStream
import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * Avro serializer for the records that are produced into Kafka
 *
 * @param avroType
 * @param topic
 * @tparam T
 */
class AvroSerializationSchema[T <: GenericRecord](avroType: Class[T], topic: String) extends KafkaSerializationSchema[T] with LazyLogging {

  lazy val schema: Schema = new Schema.Parser().parse(getClass.getResourceAsStream("/state.avsc"))

  override def serialize(element: T, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val writer = new SpecificDatumWriter[T](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(element, encoder)
    encoder.flush()
    out.close()

    new ProducerRecord[Array[Byte], Array[Byte]](topic, element.get("id").toString.getBytes, out.toByteArray)
  }
}
