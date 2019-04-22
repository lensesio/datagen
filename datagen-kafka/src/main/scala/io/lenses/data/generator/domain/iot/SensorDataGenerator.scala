package io.lenses.data.generator.domain.iot

import io.lenses.data.generator.domain.DataGenerator

import scala.util.Random

object SensorDataGenerator extends DataGenerator[SensorData] {
  val sensorIds = Array("SB01", "SB02", "SB03", "SB04")
  val dataMap: Map[String, SensorData] = sensorIds.map { it =>
    val sensor = SensorData(it, 23.0, 38.0, System.currentTimeMillis())
    (it, sensor)
  }.toMap

  override protected def generate(): Seq[(String, SensorData)] = {
    sensorIds.map { sensorId =>
      val prev = dataMap(sensorId)
      sensorId -> SensorData(sensorId,
        prev.temperature + Random.nextDouble() * 2 + Random.nextInt(2),
        prev.humidity + Random.nextDouble() * 2 * (if (Random.nextInt(2) % 2 == 0) -1 else 1),
        System.currentTimeMillis())
    }

  }
}
