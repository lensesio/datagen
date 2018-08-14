package com.landoop.data.generator.domain.weather

import java.io.ByteArrayOutputStream

import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.domain.DataGenerator
import com.landoop.data.generator.kafka.Producers
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.joda.time.DateTime

import scala.util.Random

object WeatherDataGenerator extends DataGenerator[WeatherS] {

  private val textValues = Vector("Sunny", "Cloudy", "Mostly Cloudy", "Snowing", "Showers", "Heavy Showers")
  private val dayOfWeekMap = Map(1 -> "Monday", 2 -> "Tuesday", 3 -> "Wednesday", 4 -> "Thursday", 5 -> "Friday", 6 -> "Saturday", 7 -> "Sunday")

  private def randWeather() = {
    val now = DateTime.now()
    val forecasts = (1 to 10).map { i =>
      val d = now.plusDays(1)
      ForecastS(d.toString(), dayOfWeekMap(d.dayOfWeek().get()), Random.nextInt(100), Random.nextInt(), textValues(Random.nextInt(textValues.size)))
    }.toList


    WeatherS(
      WindS(Random.nextInt(), Random.nextInt(360), Random.nextInt(200)),
      AtmosphereS(Random.nextInt(100), 100 * Random.nextDouble(), Random.nextInt(), Random.nextDouble() * 100),
      forecasts)
  }

  override protected def generate(): Seq[(String, WeatherS)] = {
    val cities = List("London", "New York", "Paris", "Barcelona", "Tokyo", "Athens", "Sibiu")
    cities.map { c => c -> randWeather() }
  }

  private def randWeatherJ() = {
    val now = DateTime.now()
    val forecasts = (1 to 10).map { i =>
      val d = now.plusDays(1)
      WeatherOuterClass.Forecast.newBuilder()
        .setDate(d.toString())
        .setDay(dayOfWeekMap(d.dayOfWeek().get()))
        .setHigh(Random.nextInt(100))
        .setLow(Random.nextInt())
        .setText(textValues(Random.nextInt(textValues.size)))
        .build()
    }.toList


    val wind = WeatherOuterClass.Wind.newBuilder()
      .setChill(Random.nextInt())
      .setDirection(Random.nextInt(360))
      .setSpeed(Random.nextInt(200))
      .build()

    val atmosphere = WeatherOuterClass.Atmosphere.newBuilder()
      .setHumidity(Random.nextInt(100))
      .setPressure(100 * Random.nextDouble())
      .setRising(Random.nextInt())
      .setVisibility(100 * Random.nextDouble())
      .build()

    val builder = WeatherOuterClass.Weather.newBuilder()
      .setWind(wind)
      .setAtmosphere(atmosphere)

    forecasts.foreach(builder.addForecast)
    builder.build()
  }


  override def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getProducerProps(classOf[StringSerializer], classOf[ByteArraySerializer])
    implicit val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](props)

    logger.info(s"Publishing sensor data to '$topic'")
    try {
      val cities = List("London", "New York", "Paris", "Barcelona", "Tokyo", "Athens", "Sibiu")
      while (true) {
        cities.map { c => c -> randWeatherJ() }.foreach { case (k, v) =>
          val bos = new ByteArrayOutputStream()
          v.writeTo(bos)
          val value = bos.toByteArray

          producer.send(new ProducerRecord(topic, k, value))
          bos.close()
        }
        Thread.sleep(config.pauseBetweenRecordsMs)
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"Failed to publish credit card data to '$topic'", t)
    }
    producer.close()
  }
}
