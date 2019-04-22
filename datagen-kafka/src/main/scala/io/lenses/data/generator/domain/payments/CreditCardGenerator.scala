package io.lenses.data.generator.domain.payments

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
import org.apache.kafka.common.serialization.StringSerializer
import pbdirect._

object CreditCardGenerator extends Generator with StrictLogging {

  def avro(topic: String)(implicit config: DataGeneratorConfig) = {
    val props = Producers.getAvroValueProducerProps(classOf[StringSerializer])
    val producer = new KafkaProducer[Any, Any](props)
    val rf = RecordFormat[CreditCard]

    logger.info(s"Publishing credit card data to '$topic'")
    try {
      CreditCard.Cards.foreach { cc =>
        val record = new ProducerRecord[Any, Any](topic, cc.number, rf.to(cc))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating credit card data to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def json(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[StringSerializer])
    val producer = new KafkaProducer[Any, Any](props)

    logger.info(s"Publishing credit card data to '$topic'")
    try {
      CreditCard.Cards.foreach { cc =>
        val record = new ProducerRecord[Any, Any](topic, cc.number, JacksonJson.toJson(cc))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating credit card data to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def xml(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[StringSerializer])
    val producer = new KafkaProducer[Any, Any](props)

    logger.info(s"Publishing credit card data to '$topic'")
    try {
      CreditCard.Cards.foreach { cc =>
        val record = new ProducerRecord[Any, Any](topic, cc.number, JacksonXml.toXml(cc))
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating credit card data to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getProducerProps(classOf[StringSerializer], classOf[ByteArraySerializer])
    implicit val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer(props)

    logger.info(s"Publishing credit card data to '$topic'")
    try {
      CreditCard.Cards.foreach { cc =>
        val record = new ProducerRecord(topic, cc.number, cc.toPB)
        producer.send(record)
      }
      producer.close()

      logger.info(s"Finished generating credit card data to '$topic'")
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }
}
