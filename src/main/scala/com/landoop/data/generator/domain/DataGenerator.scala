package com.landoop.data.generator.domain

import java.nio.ByteBuffer

import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.json.{JacksonJson, JacksonXml}
import com.landoop.data.generator.kafka.Producers
import com.sksamuel.avro4s.{RecordFormat, ScaleAndPrecision, ToValue}
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.{Conversions, LogicalTypes}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import pbdirect._

import DataGenerator._
abstract class DataGenerator[T](implicit rf: RecordFormat[T], pbWriter: PBWriter[T]) extends Generator with StrictLogging {

  protected def generate(): Seq[(String, T)]

  private def generate[V](topic: String, delay: Long)(thunk: T => V)(implicit producer: KafkaProducer[String, V]): Unit = {
    Iterator.continually(generate()).flatten.foreach { case (k, v) =>
      val record = new ProducerRecord(topic, k, thunk(v))
      producer.send(record)
      Thread.sleep(delay)
    }
  }

  override def avro(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getAvroValueProducerProps(classOf[StringSerializer])
    implicit val producer: KafkaProducer[String, GenericRecord] = new KafkaProducer[String, GenericRecord](props)


    logger.info(s"Publishing sensor data to '$topic'")
    try {
      generate(topic, config.pauseBetweenRecordsMs)(rf.to)
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def json(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[StringSerializer])
    implicit val producer = new KafkaProducer[String, String](props)

    logger.info(s"Publishing sensor data to '$topic'")
    try {
      generate(topic, config.pauseBetweenRecordsMs)(JacksonJson.toJson)
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def xml(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[StringSerializer])
    implicit val producer = new KafkaProducer[String, String](props)

    logger.info(s"Publishing sensor data to '$topic'")
    try {
      generate(topic, config.pauseBetweenRecordsMs)(JacksonXml.toXml)
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getProducerProps(classOf[StringSerializer], classOf[ByteArraySerializer])
    implicit val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](props)

    logger.info(s"Publishing sensor data to '$topic'")
    try {
      generate(topic, config.pauseBetweenRecordsMs) { v =>
        v.toPB
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }
}


object DataGenerator {

  import cats.syntax.invariant._

  implicit val instantFormat: PBFormat[BigDecimal] =
    PBFormat[String].imap(BigDecimal(_))(_.toString())

  implicit def BigDecimalToValue(implicit sp: ScaleAndPrecision = ScaleAndPrecision(18,38)): ToValue[BigDecimal] = {
    val decimalConversion = new Conversions.DecimalConversion
    val decimalType = LogicalTypes.decimal(sp.precision, sp.scale)
    new ToValue[BigDecimal] {
      override def apply(value: BigDecimal): ByteBuffer = {
        decimalConversion.toBytes(value.bigDecimal, null, decimalType)
      }
    }
  }

}