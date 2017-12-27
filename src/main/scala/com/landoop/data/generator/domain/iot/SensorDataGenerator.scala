package com.landoop.data.generator.domain.iot

import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.domain.Generator
import com.landoop.data.generator.json.JacksonJson
import com.landoop.data.generator.kafka.Producers
import com.sksamuel.avro4s.RecordFormat
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

object SensorDataGenerator extends Generator with StrictLogging {
  val sensorIds = Array("SB01", "SB02", "SB03", "SB04")
  val dataMap = sensorIds.map { it =>
    val sensor = SensorData(it, 23.0, 38.0, System.currentTimeMillis())
    (it, sensor)
  }.toMap

  override def avro(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getAvroValueProducerProps(classOf[StringSerializer])
    val producer = new KafkaProducer[String, GenericRecord](props)
    val rf = RecordFormat[SensorData]

    logger.info(s"Publishing sensor data to '$topic'")
    try {
      while (true) {
        sensorIds.foreach { sensorId =>
          val prev = dataMap(sensorId)
          val sensorData = SensorData(sensorId,
            prev.temperature + Random.nextDouble() * 2 + Random.nextInt(2),
            prev.humidity + Random.nextDouble() * 2 * (if (Random.nextInt(2) % 2 == 0) -1 else 1),
            System.currentTimeMillis())
          val record = new ProducerRecord(topic, sensorId, rf.to(sensorData))
          producer.send(record)
        }
        Thread.sleep(500 + Random.nextInt(101))
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def json(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[StringSerializer])
    val producer = new KafkaProducer[String, String](props)

    logger.info(s"Publishing sensor data to '$topic'")
    try {
      while (true) {
        sensorIds.foreach { sensorId =>
          val prev = dataMap(sensorId)
          val sensorData = SensorData(sensorId,
            prev.temperature + Random.nextDouble() * 2 + Random.nextInt(2),
            prev.humidity + Random.nextDouble() * 2 * (if (Random.nextInt(2) % 2 == 0) -1 else 1),
            System.currentTimeMillis())
          val record = new ProducerRecord(topic, sensorId, JacksonJson.toJson(sensorData))
          producer.send(record)
        }
        Thread.sleep(500 + Random.nextInt(101))
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }
}
