package io.lenses.data.generator.domain.iot

import io.lenses.data.generator.domain.DataGenerator

import scala.util.Random

object DeviceTemperatureDataGenerator extends DataGenerator[DeviceTemperature] {
  private val devices = (1 to 100).map { i => s"cD$i" }.toVector

  override protected def generate(): Seq[(String, DeviceTemperature)] = {
    devices.map { d =>
      val device = DeviceTemperature(
        d,
        "Temperature",
        List(Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt()))
      d -> device
    }
  }
}
