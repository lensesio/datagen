package com.landoop.data.generator.domain.iot

import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.domain.Generator
import com.landoop.data.generator.json.{JacksonJson, JacksonXml}
import com.landoop.data.generator.kafka.Producers
import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericContainer, GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.util.Random

object DeviceTemperatureArrayDataGenerator extends Generator with StrictLogging {
  private val devices = (1 to 10).map { i => s"cD$i" }.toList

  protected def generate(): List[DeviceTemperature] = {
    devices.map { d =>
      DeviceTemperature(
        d,
        "Temperature",
        List(Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt()))
    }
  }


  private def generate[V](topic: String, delay: Long)(thunk: List[DeviceTemperature] => V)(implicit producer: KafkaProducer[String, V]): Unit = {
    Iterator.continually(generate()).foreach { devices =>
      val record = new ProducerRecord(topic, DateTime.now.toString(), thunk(devices))
      producer.send(record)
      Thread.sleep(delay)
    }
  }

  override def avro(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getAvroValueProducerProps(classOf[StringSerializer])
    implicit val producer: KafkaProducer[String, GenericContainer] = new KafkaProducer[String, GenericContainer](props)

    logger.info(s"Publishing sensor data to '$topic'")
    val deviceSchema = SchemaFor[DeviceTemperature]()
    val schema = SchemaBuilder.array().items(deviceSchema)
    val rf = RecordFormat[DeviceTemperature]
    try
      generate(topic, config.pauseBetweenRecordsMs) { devices: List[DeviceTemperature] =>
        new GenericData.Array[GenericRecord](schema, devices.map(rf.to).asJava).asInstanceOf[GenericContainer]
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
      generate(topic, config.pauseBetweenRecordsMs)(d=>JacksonXml.toXml(d.asJava))
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }
}
