package io.lenses.data.generator.cli

import caseapp._
import io.lenses.data.generator.FormatType
import cats.effect.Resource.Par
import caseapp.core.argparser.SimpleArgParser
import caseapp.core.argparser.ArgParser
import caseapp.core.Error
import org.http4s.Uri

sealed trait Command

final case class LensesCreds(user: String, password: String)
object LensesCreds {
  implicit val parser: ArgParser[LensesCreds] =
    SimpleArgParser.from("user:password") { s =>
      s.split(':').toList match {
        case username :: password :: _ if password.nonEmpty =>
          Right(LensesCreds(username, password))
        case _ =>
          Left(Error.MalformedValue(s, s"cannot parse lenses creds"))
      }
    }
}

final case class GenSchemas(
    @HelpMessage("Number of kafka topics to generate")
    numKafkaDatasets: Int,
    @HelpMessage("Number of Elasticsearch indexes to generate")
    numElasticDatasets: Int,
    @HelpMessage("Lenses base URL (defaults to http://localhost:24015)")
    lensesBaseUrl: Uri = Uri.unsafeFromString("http://localhost:24015"),
    @HelpMessage("Lenses user credentials (defaults to admin:admin)")
    lensesCreds: LensesCreds = LensesCreds("admin", "admin"),
    @HelpMessage("Elasticsearch base URL (defaults to http://localhost:9200)")
    elasticsearchBaseurl: Uri = Uri.unsafeFromString("http://localhost:9200")
) extends Command

final case class GenRecords(
    @HelpMessage("Kafka topic to publish to")
    topic: String,
    @HelpMessage("Topic partitions")
    partitions: Int = 1,
    @HelpMessage("Topic replication factor")
    replication: Int = 1,
    @HelpMessage("Dataset type")
    dataSet: Int = 1,
    @HelpMessage("Data format: AVRO/JSON/XML")
    format: FormatType = FormatType.JSON,
    @HelpMessage("Kafka bootstrap broker URLs")
    brokers: String,
    @HelpMessage("Schema registry URLs")
    schema: String,
    @HelpMessage("Is default schema mode")
    schemaMode: Boolean = true,
    @HelpMessage("Sleep duration (ms) in between each message published")
    produceDelay: Long = 1
) extends Command
