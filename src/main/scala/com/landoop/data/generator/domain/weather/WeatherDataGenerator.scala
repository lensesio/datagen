package com.landoop.data.generator.domain.weather

import com.landoop.data.generator.domain.DataGenerator
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.joda.time.DateTime

import scala.util.Random

object WeatherDataGenerator extends DataGenerator[Weather] with StrictLogging {
  private val textValues = Vector("Sunny", "Cloudy", "Mostly Cloudy", "Snowing", "Showers", "Heavy Showers")
  private val dayOfWeekMap = Map(1 -> "Monday", 2 -> "Tuesday", 3 -> "Wednesday", 4 -> "Thursday", 5 -> "Friday", 6 -> "Saturday", 7 -> "Sunday")

  private def randWeather() = {
    val now = DateTime.now()
    val forecasts = (1 to 10).map { i =>
      val d = now.plusDays(1)
      Forecast(d.toString(), dayOfWeekMap(d.dayOfWeek().get()), Random.nextInt(100), Random.nextInt(), textValues(Random.nextInt(textValues.size)))
    }.toList


    Weather(
      Wind(Random.nextInt(), Random.nextInt(360), Random.nextInt(200)),
      Atmosphere(Random.nextInt(100), 100 * Random.nextDouble(), Random.nextInt(), Random.nextDouble() * 100),
      forecasts)
  }

  override protected def generate(): Seq[(String, Weather)] = {
    val cities = List("London", "New York", "Paris", "Barcelona", "Tokyo", "Athens", "Sibiu")
    cities.map { c => c -> randWeather() }
  }
}
