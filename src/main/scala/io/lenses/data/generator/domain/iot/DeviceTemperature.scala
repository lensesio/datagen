package io.lenses.data.generator.domain.iot

import com.sksamuel.avro4s.RecordFormat

case class DeviceTemperature(id: String, sensorType: String, values: List[Int])

object DeviceTemperature {
  implicit val recordFormat: RecordFormat[DeviceTemperature] = RecordFormat[DeviceTemperature]
}