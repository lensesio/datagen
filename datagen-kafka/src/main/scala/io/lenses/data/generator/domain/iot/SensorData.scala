package io.lenses.data.generator.domain.iot

import com.sksamuel.avro4s.RecordFormat

case class SensorData(id: String, temperature: Double, humidity: Double, timestamp: Long)

object SensorData {
  implicit val recordFormat: RecordFormat[SensorData] = RecordFormat[SensorData]
}