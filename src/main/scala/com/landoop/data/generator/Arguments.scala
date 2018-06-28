package com.landoop.data.generator

case class Arguments(topic: String,
                     partitions: Int,
                     replication: Int,
                     dataSet: Int,
                     format: FormatType,
                     brokers: String,
                     schemaRegistry: String,
                     defaultSchemaRegistry: Boolean = true,
                     produceDelay: Long = 100L)