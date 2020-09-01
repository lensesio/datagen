package io.lenses.data.generator.domain.weather

import com.sksamuel.avro4s.RecordFormat

case class WindS(chill: Int, direction: Int, speed: Int)

case class AtmosphereS(humidity: Int, pressure: Double, rising: Int, visibility: Double)

case class ForecastS(date: String, day: String, high: Int, low: Int, text: String)

case class WeatherS(wind: WindS, atmosphere: AtmosphereS, forecasts: List[ForecastS])

object WeatherS {
  implicit val recordFormat: RecordFormat[WeatherS] = RecordFormat[WeatherS]
}