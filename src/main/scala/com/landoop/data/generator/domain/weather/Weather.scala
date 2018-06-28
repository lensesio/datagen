package com.landoop.data.generator.domain.weather

case class Wind(chill: Int, direction: Int, speed: Int)

case class Atmosphere(humidity: Int, pressure: Double, rising: Int, visibility: Double)

case class Forecast(date: String, day: String, high: Int, low: Int, text:String)

case class Weather(wind: Wind, atmosphere: Atmosphere, forecasts: List[Forecast])

