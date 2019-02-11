package com.landoop.data.generator.domain

import java.time.LocalDate
import java.util.UUID

import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.kafka.Producers
import com.sksamuel.avro4s.{AvroSchema, ToRecord, ToSchema, ToValue}
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

case class Subscription(customer_id: String, customer_name: String, subscription_date: LocalDate, expiry_date: LocalDate)

object SubscriptionGenerator extends Generator with StrictLogging {

  implicit object DateToSchema extends ToSchema[LocalDate] {
    override protected val schema: Schema = {
      val schema = Schema.create(Schema.Type.INT)
      LogicalTypes.date().addToSchema(schema)
      schema
    }
  }

  implicit object DateToValue extends ToValue[LocalDate] {
    override def apply(value: LocalDate): Any = value.toEpochDay.toInt
  }

  val schema: Schema = AvroSchema[Subscription]
  println(schema.toString(true))
  val rf: ToRecord[Subscription] = ToRecord[Subscription]

  def generate: Subscription = {
    val date = LocalDate.of(2018, 1, 1).plusDays(Random.nextInt(31)).plusMonths(Random.nextInt(14))
    Subscription(UUID.randomUUID().toString, names(Random.nextInt(names.length)), date, date.plusMonths(Random.nextInt(6) + 1))
  }

  private val names = List(
    "Jim Hawkins",
    "Billy Bones",
    "John Trelawney",
    "Long John Silver",
    "Alexander Smollett",
    "Ben Gunn",
    "Israel Hands",
    "Tom Redruth",
    "David Livesey",
    "Abraham Gray",
    "Tom Morgan",
    "Dick Johnson",
    "Richard Joyce",
    "George Merry"
  )

  override def avro(topic: String)(implicit config: DataGeneratorConfig): Unit = {

    val props = Producers.getAvroValueProducerProps(classOf[StringSerializer])
    implicit val producer: KafkaProducer[String, Any] = new KafkaProducer[String, Any](props)

    logger.info(s"Publishing subscription data to '$topic'")


    try {
      Iterator.continually(generate).foreach { subscription =>
        val container = rf(subscription)
        val record = new ProducerRecord[String, Any](topic, container)
        producer.send(record)
        Thread.sleep(config.pauseBetweenRecordsMs)
      }
    } catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def json(topic: String)(implicit config: DataGeneratorConfig): Unit = ???
  override def xml(topic: String)(implicit config: DataGeneratorConfig): Unit = ???
  override def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit = ???
}
