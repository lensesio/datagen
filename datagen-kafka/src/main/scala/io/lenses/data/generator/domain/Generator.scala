package io.lenses.data.generator.domain

import io.lenses.data.generator.config.DataGeneratorConfig

trait Generator {
  def avro(topic: String)(implicit config: DataGeneratorConfig): Unit

  def json(topic: String)(implicit config: DataGeneratorConfig): Unit

  def xml(topic: String)(implicit config: DataGeneratorConfig): Unit

  def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit
}
