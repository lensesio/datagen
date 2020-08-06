package io.lenses.data.generator.domain.bikesharing

import com.sksamuel.avro4s.RecordFormat
import com.typesafe.scalalogging.StrictLogging
import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.domain.Generator
import io.lenses.data.generator.json.JacksonJson
import io.lenses.data.generator.json.JacksonXml
import io.lenses.data.generator.kafka.Producers
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.IntegerSerializer
import pbdirect._

object StationsGenerator extends Generator with StrictLogging {

  def avro(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getAvroValueProducerProps(classOf[IntegerSerializer])
    val producer = new KafkaProducer[Any, Any](props)
    val rf = RecordFormat[Station]

    logger.info(s"Publishing bike sharing station data to '$topic'")
    try {
      Station.fromDb().foreach { station =>
        val record = new ProducerRecord[Any, Any](topic, station.id, rf.to(station))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating bike sharing station to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish bike sharing station to '$topic'", t)
    }
  }

  override def json(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[IntegerSerializer])
    val producer = new KafkaProducer[Any, Any](props)

    logger.info(s"Publishing bike sharing station to '$topic'")
    try {
      Station.fromDb().foreach { station =>
        val record = new ProducerRecord[Any, Any](topic, station.id, JacksonJson.toJson(station))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating bike sharing station to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish bike sharing station to '$topic'", t)
    }
  }

  override def xml(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[IntegerSerializer])
    val producer = new KafkaProducer[Any, Any](props)

    logger.info(s"Publishing bike sharing station to '$topic'")
    try {
      Station.fromDb().foreach { station =>
        val record = new ProducerRecord[Any, Any](topic, station.id, JacksonXml.toXml(station))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating bike sharing station to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish bike sharing station to '$topic'", t)
    }
  }

  override def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getProducerProps(classOf[IntegerSerializer], classOf[ByteArraySerializer])
    implicit val producer: KafkaProducer[Int, Array[Byte]] = new KafkaProducer(props)

    logger.info(s"Publishing bike sharing station to '$topic'")
    try {
      Station.fromDb().foreach { station =>
        val record = new ProducerRecord(topic, station.id, station.toPB)
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating bike sharing station to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish bike sharing station to '$topic'", t)
    }
  }
}
