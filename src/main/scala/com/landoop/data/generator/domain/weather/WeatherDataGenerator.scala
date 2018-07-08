package com.landoop.data.generator.domain.weather

import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.domain.DataGenerator
import com.landoop.data.generator.domain.weather.WeatherOuterClass.{Atmosphere, Forecast, Weather, Wind}
import com.landoop.data.generator.kafka.Producers
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.joda.time.DateTime

import scala.util.Random

object WeatherDataGenerator extends DataGenerator[WeatherOuterClass.Weather] {
  private val textValues = Vector("Sunny", "Cloudy", "Mostly Cloudy", "Snowing", "Showers", "Heavy Showers")
  private val dayOfWeekMap = Map(1 -> "Monday", 2 -> "Tuesday", 3 -> "Wednesday", 4 -> "Thursday", 5 -> "Friday", 6 -> "Saturday", 7 -> "Sunday")

  private def randWeather() = {
    val now = DateTime.now()
    val forecasts = (1 to 10).map { i =>
      val d = now.plusDays(1)
      new Forecast.Builder()
        .setDate(d.toString())
        .setDay(dayOfWeekMap(d.dayOfWeek().get()))
        .setHigh(Random.nextInt(100))
        .setLow(Random.nextInt())
        .setText(textValues(Random.nextInt(textValues.size)))
        .build()
    }.toList


    val wind = new Wind.Builder()
      .setChill(Random.nextInt())
      .setDirection(Random.nextInt(360))
      .setSpeed(Random.nextInt(200))
      .build()

    val atmosphere = new Atmosphere.Builder()
      .setHumidity(Random.nextInt(100))
      .setPressure(100 * Random.nextDouble())
      .setRising(Random.nextInt())
      .setVisibility(100 * Random.nextDouble())
      .build()

    val builder = new Weather.Builder()
      .setWind(wind)
      .setAtmosphere(atmosphere)

    forecasts.zipWithIndex.foreach { case (f, i) => builder.setForecast(i, f) }
    builder.build()
  }

  override protected def generate(): Seq[(String, WeatherOuterClass.Weather)] = {
    val cities = List("London", "New York", "Paris", "Barcelona", "Tokyo", "Athens", "Sibiu")
    cities.map { c => c -> randWeather() }
  }


  override def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getProducerProps(classOf[StringSerializer], classOf[ByteArraySerializer])
    implicit val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](props)

    logger.info(s"Publishing sensor data to '$topic'")
    try {
      generate().foreach { case (k, v) =>
        val bos = new ByteOutputStream()
        v.writeTo(bos)
        val value = bos.getBytes
        producer.send(new ProducerRecord(topic, k, value))
        bos.close()
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
    producer.close()
  }
}
