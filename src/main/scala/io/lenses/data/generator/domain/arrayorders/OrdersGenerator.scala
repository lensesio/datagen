package io.lenses.data.generator.domain.arrayorders

import java.util.UUID

import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.domain.DataGenerator
import io.lenses.data.generator.kafka.Producers
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import pbdirect._

object OrdersGenerator extends DataGenerator[Order] {

  override def protobuf(
    topic: String
  )(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getProducerProps(
      classOf[StringSerializer],
      classOf[ByteArraySerializer]
    )
    implicit val producer: KafkaProducer[String, Array[Byte]] =
      new KafkaProducer[String, Array[Byte]](props)

    logger.info(s"Publishing sensor data to '$topic'")
    try {
      val cities = List(
        "London",
        "New York",
        "Paris",
        "Barcelona",
        "Tokyo",
        "Athens",
        "Sibiu"
      )
      while (true) {
        cities
          .map { c =>
            c -> randOrder(c)
          }
          .foreach {
            case (k, v) =>
              val value = v.toPB
              producer.send(new ProducerRecord(topic, k, value))
          }
        Thread.sleep(config.pauseBetweenRecordsMs)
      }
    } catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
    producer.close()
  }
  private def randOrder(hub: String) = {
    Order(
      UUID.randomUUID().toString,
      System.currentTimeMillis().toString,
      "ABC",
      hub
    )
  }
  override protected def generate(): Seq[(String, Order)] = {
    val cities = List(
      "London",
      "New York",
      "Paris",
      "Barcelona",
      "Tokyo",
      "Athens",
      "Sibiu"
    )
    cities.map { c =>
      c -> randOrder(c)
    }
  }
}
