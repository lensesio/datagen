package io.lenses.data.generator.config

case class DataGeneratorConfig(brokers: String,
                               schemaRegistry: String,
                               pauseBetweenRecordsMs: Long,
                               defaultSchemaRegistry: Boolean)
