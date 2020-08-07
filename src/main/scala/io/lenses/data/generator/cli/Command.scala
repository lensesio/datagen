package io.lenses.data.generator.cli

import caseapp._
import io.lenses.data.generator.FormatType

sealed trait Command
//
case class NewGen(numDatasets: Int) extends Command

// @AppName("Datagen")
// @AppVersion("0.1.0")
// @ProgName("oldgen")
case class OldGen(
    // @HelpMessage("Kafka topic to publish to")
    topic: String,
    // @HelpMessage("Topic partitions")
    partitions: Int = 1,
    // @HelpMessage("Topic replication factor")
    replication: Int = 1,
    // @HelpMessage("Dataset type")
    dataSet: Int = 1,
    // @HelpMessage("Data format: AVRO/JSON/XML")
    format: FormatType = FormatType.JSON,
    // @HelpMessage("Kafka bootstrap broker URLs")
    brokers: String,
    // @HelpMessage("Schema registry URLs")
    schema: String,
    // @HelpMessage("Is default schema mode")
    schemaMode: Boolean = true,
    // @HelpMessage("Sleep duration (ms) in between each message published")
    produceDelay: Long = 1
) extends Command
