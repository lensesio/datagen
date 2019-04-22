package io.lenses.data.generator.domain

import com.sksamuel.avro4s.Record
import com.sksamuel.avro4s.RecordFormat
import com.typesafe.scalalogging.StrictLogging
import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.json.JacksonJson
import io.lenses.data.generator.json.JacksonXml
import io.lenses.data.generator.kafka.Producers
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import pbdirect._

abstract class DataGenerator[T](implicit rf: RecordFormat[T], pbWriter: PBWriter[T])
  extends Generator
    with StrictLogging {

  protected def generate(): Seq[(String, T)]

  private def generate[V](topic: String, delay: Long)(thunk: T => V)
                         (implicit producer: KafkaProducer[String, V]): Unit = {
    Iterator.continually(generate()).flatten.foreach { case (k, v) =>
      val record = new ProducerRecord(topic, k, thunk(v))
      producer.send(record)
      Thread.sleep(delay)
    }
  }

  override def avro(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getAvroValueProducerProps(classOf[StringSerializer])
    implicit val producer: KafkaProducer[String, Record] = new KafkaProducer[String, Record](props)

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
    implicit val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

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

}