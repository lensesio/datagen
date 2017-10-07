package com.landoop.data.generator.domain.payments

import java.time.LocalDate

import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.domain.Generator
import com.landoop.data.generator.json.JacksonJson
import com.landoop.data.generator.kafka.Producers
import com.sksamuel.avro4s.{RecordFormat, ScaleAndPrecision}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object PaymentsGenerator extends Generator with StrictLogging {
  private val MerchantIds = (1 to 100).map(_.toLong).toVector
  private val DateFormatter = ISODateTimeFormat.dateTime()

  override def avro(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getAvroValueProducerProps(classOf[StringSerializer])
    val producer = new KafkaProducer[Any, Any](props)
    implicit val sp = ScaleAndPrecision(18, 38)
    val rf = RecordFormat[Payment]

    logger.info(s"Publishing payments data to '$topic'")
    try {
      while (true) {

        val index = Random.nextInt(CreditCard.Cards.size)
        val cc = CreditCard.Cards(index)

        val dt = new DateTime().toDateTime(DateTimeZone.UTC)
        val date = DateFormatter.print(dt)

        val left = 10 + Random.nextInt(5000)
        val right = Random.nextInt(100)
        var decimal = BigDecimal(s"$left.$right").setScale(18, RoundingMode.HALF_UP)
        val payment = Payment(s"txn${System.currentTimeMillis()}", date, decimal, cc.currency, cc.number, MerchantIds(Random.nextInt(MerchantIds.size)))
        val record = new ProducerRecord[Any, Any](topic, cc.number, rf.to(payment))
        producer.send(record)
        Thread.sleep(50 + Random.nextInt(101))
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }

  override def json(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getStringValueProducerProps(classOf[StringSerializer])
    val producer = new KafkaProducer[Any, Any](props)

    logger.info(s"Publishing payments data to '$topic'")
    try {
      while (true) {

        val index = Random.nextInt(CreditCard.Cards.size)
        val cc = CreditCard.Cards(index)
        import java.time.format.DateTimeFormatter

        val date = LocalDate.now().format(DateTimeFormatter.ISO_DATE_TIME)
        val payment = Payment(s"txn${System.currentTimeMillis()}", date, BigDecimal(Math.random()), cc.currency, cc.number, MerchantIds(Random.nextInt(MerchantIds.size)))
        val record = new ProducerRecord[Any, Any](topic, cc.number, JacksonJson.toJson(payment))
        producer.send(record)
        Thread.sleep(50 + Random.nextInt(101))
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
  }
}
