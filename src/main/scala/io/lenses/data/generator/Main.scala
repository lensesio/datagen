package io.lenses.data.generator

import java.net.URL

import com.typesafe.scalalogging.StrictLogging
import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.domain.SubscriptionGenerator
import io.lenses.data.generator.domain.iot.DeviceTemperatureArrayDataGenerator
import io.lenses.data.generator.domain.iot.SensorDataGenerator
import io.lenses.data.generator.domain.payments.PaymentsGenerator
import io.lenses.data.generator.domain.weather.WeatherDataGenerator
import io.lenses.data.generator.domain.Generator
import io.lenses.data.generator.domain.arrayorders.OrdersGenerator
import io.lenses.data.generator.domain.bikesharing.StationsGenerator
import io.lenses.data.generator.domain.bikesharing.TripsGenerator
import io.lenses.data.generator.domain.iot.DeviceTemperatureDataGenerator
import io.lenses.data.generator.domain.payments.CreditCardGenerator
import io.lenses.data.generator.domain.recursive.CustomerGenerator
import io.lenses.data.generator.cli._
import scala.util.Try
import caseapp._
import io.lenses.data.generator.schema.Schema
import io.lenses.data.generator.schema.Record
import io.lenses.data.generator.http.LensesClient
import org.http4s.Uri
import org.http4s.client.blaze.BlazeClientBuilder
import cats.effect.IO
import cats.effect.ContextShift
import cats.implicits._
import org.apache.avro.io.EncoderFactory
import io.lenses.data.generator.schema.AvroConverter
import io.lenses.data.generator.schema.DatasetCreator
import org.http4s.client.middleware.Logger
import io.lenses.data.generator.schema.pg.PostgresConfig
import doobie.util.transactor.Transactor
import cats.effect.Blocker

object Main extends caseapp.CommandApp[Command] with StrictLogging {

  override val appName = "Lenses Datagen"
  override val appVersion = "0.1.0"
  override val progName = "datagen"

  lazy val generators = Map[Int, Generator](
    1 -> CreditCardGenerator,
    2 -> PaymentsGenerator,
    3 -> SensorDataGenerator,
    4 -> WeatherDataGenerator,
    5 -> DeviceTemperatureDataGenerator,
    6 -> DeviceTemperatureArrayDataGenerator,
    7 -> SubscriptionGenerator,
    8 -> StationsGenerator,
    9 -> TripsGenerator,
    10 -> CustomerGenerator,
    11 -> OrdersGenerator
  )

  def run(command: Command, otherArgs: RemainingArgs) =
    command match {
      case GenSchemas(
            numKafkaDatasets,
            numElasticDatasets,
            numPostgresDatasets,
            lensesBaseUrl,
            lensesCreds,
            elasticsearchBaseUrl,
            maybeElasticCreds,
            postgresSchema,
            postgresDatabase,
            postgresCreds
          ) =>
        import org.scalacheck.Gen
        import io.lenses.data.generator.schema.Gens

        val ec = scala.concurrent.ExecutionContext.global
        implicit val cs: ContextShift[IO] = IO.contextShift(ec)
        val config =
          schema.Gens.Config(recordsOnly = true, maxDepth = 3)

        val maybeTransactor = postgresDatabase.map { db =>
          Transactor.fromDriverManager[IO](
            "org.postgresql.Driver", // driver classname
            s"jdbc:postgresql:$db", // connect URL (driver-specific)
            postgresCreds.user,
            postgresCreds.password,
            Blocker.liftExecutionContext(ec)
          )
        }

        BlazeClientBuilder[IO](ec).resource
          .use { httpClient0 =>
            val httpClient = Logger(true, true)(httpClient0)
            val lensesClient = LensesClient(
              lensesBaseUrl,
              httpClient
            )

            fs2
              .Stream(
                maybeTransactor.fold(fs2.Stream.eval(IO.unit)) { xa =>
                  Gens
                    .namedSchemas(numPostgresDatasets, config)
                    .evalMap {
                      case (datasetName, schema) =>
                        DatasetCreator
                          .Postgres(
                            xa,
                            PostgresConfig(
                              Some("baz"),
                              addPrimaryKey = true
                            )
                          )
                          .create(datasetName, schema)(ec, cs)
                    }
                },
                Gens
                  .namedSchemas(numKafkaDatasets, config)
                  .parEvalMap(3) {
                    case (datasetName, schema) =>
                      DatasetCreator
                        .Kafka(lensesClient, lensesCreds)
                        .create(datasetName, schema)(ec, cs)
                  },
                Gens
                  .namedSchemas(numElasticDatasets, config)
                  .parEvalMap(3) {
                    case (datasetName, schema) =>
                      DatasetCreator
                        .Elasticsearch(
                          httpClient,
                          elasticsearchBaseUrl,
                          maybeElasticCreds
                        )
                        .create(datasetName, schema)(ec, cs)
                  }
              )
              .parJoinUnbounded
              .compile
              .drain

          }
          .unsafeRunSync()

      case oldGen: GenRecords =>
        implicit val generatorConfig: DataGeneratorConfig = DataGeneratorConfig(
          oldGen.brokers,
          oldGen.schema,
          oldGen.produceDelay,
          oldGen.schemaMode
        )

        CreateTopicFn(
          oldGen
        )

        val generator = generators(oldGen.dataSet)
        oldGen.format match {
          case FormatType.AVRO  => generator.avro(oldGen.topic)
          case FormatType.JSON  => generator.json(oldGen.topic)
          case FormatType.XML   => generator.xml(oldGen.topic)
          case FormatType.PROTO => generator.protobuf(oldGen.topic)
        }
    }

}
