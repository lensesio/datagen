package io.lenses.data.generator.cli

import io.lenses.data.generator.FormatType

final case class Command(
    topic: String,
    partitions: Int,
    replication: Int,
    dataSet: Int,
    format: FormatType,
    brokers: String,
    schemaRegistry: String,
    defaultSchemaRegistry: Boolean,
    produceDelay: Long = 1
)
