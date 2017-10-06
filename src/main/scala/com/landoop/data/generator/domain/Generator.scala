package com.landoop.data.generator.domain

import com.landoop.data.generator.config.DataGeneratorConfig

trait Generator {
  def avro(topic: String)(implicit config: DataGeneratorConfig): Unit

  def json(topic: String)(implicit config: DataGeneratorConfig):Unit
}
