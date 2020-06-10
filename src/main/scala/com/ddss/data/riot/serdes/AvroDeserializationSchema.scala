package com.ddss.data.riot.serdes

import com.typesafe.scalalogging.LazyLogging
import iot.state.State
import iot.trigger.Trigger
import org.apache.avro.Schema
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Deserialization class for the records consumed using the Kafka source
 *
 * @param clazz  type class of the records. Could be [[State]] or [[Trigger]]
 * @tparam T
 */
class AvroDeserializationSchema[T](val clazz: Class[T]) extends KafkaDeserializationSchema[T] with LazyLogging {

  lazy val schemaState: Schema = new Schema.Parser().parse(getClass.getResourceAsStream("/state.avsc"))
  lazy val schemaTrigger: Schema = new Schema.Parser().parse(getClass.getResourceAsStream("/trigger.avsc"))

  override def isEndOfStream(nextElement: T): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): T = {
    logger.debug("consuming record. partition={}, offset={}", record.partition(), record.offset())

    val decoder: Decoder = DecoderFactory.get().binaryDecoder(record.value(), null)
    getReader.read(null.asInstanceOf[T], decoder)
  }

  override def getProducedType: TypeInformation[T] = TypeExtractor.getForClass(clazz)

  private def getSchema: Schema = {
    if (clazz.getTypeName.equals(classOf[State].getTypeName)) schemaState
    else if (clazz.getTypeName.equals(classOf[Trigger].getTypeName)) schemaTrigger
    else throw new IllegalArgumentException("Invalid type")
  }

  private def getReader: DatumReader[T] = {
    new SpecificDatumReader[T](getSchema)
  }

}
