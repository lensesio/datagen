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

object TripsGenerator extends Generator with StrictLogging {

  def avro(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getAvroValueProducerProps(classOf[IntegerSerializer])
    val producer = new KafkaProducer[Any, Any](props)
    val rf = RecordFormat[Trip]

    logger.info(s"Publishing bike sharing trip data to '$topic'")
    try {
      Trip.fromDb().foreach { trip =>
        val record = new ProducerRecord[Any, Any](topic, trip.id, rf.to(trip))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating bike sharing trip data to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish bike sharing trip data to '$topic'", t)
    }
  }

  override def json(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[IntegerSerializer])
    val producer = new KafkaProducer[Any, Any](props)

    logger.info(s"Publishing bike sharing trip data to '$topic'")
    try {
      Trip.fromDb().foreach { trip =>
        val record = new ProducerRecord[Any, Any](topic, trip.id, JacksonJson.toJson(trip))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating bike sharing trip data to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish bike sharing trip data to '$topic'", t)
    }
  }

  override def xml(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[IntegerSerializer])
    val producer = new KafkaProducer[Any, Any](props)

    logger.info(s"Publishing bike sharing trip data to '$topic'")
    try {
      Trip.fromDb().foreach { trip =>
        val record = new ProducerRecord[Any, Any](topic, trip.id, JacksonXml.toXml(trip))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating bike sharing trip data to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish bike sharing trip data to '$topic'", t)
    }
  }

  override def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    /*val props = Producers.getProducerProps(classOf[IntegerSerializer], classOf[ByteArraySerializer])
    implicit val producer: KafkaProducer[Int, Array[Byte]] = new KafkaProducer(props)

    logger.info(s"Publishing bike sharing trip data to '$topic'")
    try {
      Trip.fromDb().foreach { trip =>
        val record = new ProducerRecord(topic, trip.id, trip.toPB)
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating bike sharing trip data to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish bike sharing trip data to '$topic'", t)
    }*/
  }
}
