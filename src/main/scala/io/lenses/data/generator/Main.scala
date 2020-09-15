package io.lenses.data.generator

import caseapp._
import cats.effect.{Blocker, ContextShift, IO}
import com.typesafe.scalalogging.StrictLogging
import doobie.util.transactor.Transactor
import io.lenses.data.generator.cli._
import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.domain.{Generator, SubscriptionGenerator}
import io.lenses.data.generator.domain.arrayorders.OrdersGenerator
import io.lenses.data.generator.domain.bikesharing.{
  StationsGenerator,
  TripsGenerator
}
import io.lenses.data.generator.domain.iot.{
  DeviceTemperatureArrayDataGenerator,
  DeviceTemperatureDataGenerator,
  SensorDataGenerator
}
import io.lenses.data.generator.domain.payments.{
  CreditCardGenerator,
  PaymentsGenerator
}
import io.lenses.data.generator.domain.recursive.CustomerGenerator
import io.lenses.data.generator.domain.weather.WeatherDataGenerator
import io.lenses.data.generator.http.LensesClient
import io.lenses.data.generator.schema.DatasetCreator
import io.lenses.data.generator.schema.pg.PostgresConfig
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.Logger
import io.lenses.data.generator.domain.extremes.ExtremeCaseGenerator

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
    11 -> OrdersGenerator,
    12 -> ExtremeCaseGenerator
      .sizedMessageGenerator(messageSize = 470000, messagesAmount = 20),
    13 -> ExtremeCaseGenerator
      .sizedMessageGenerator(messageSize = 970000, messagesAmount = 20),
    14 -> ExtremeCaseGenerator.nestedMessageGenerator(messagesAmount = 20)
  )

  def run(command: Command, otherArgs: RemainingArgs) =
    command match {
      case GenSchemas(
            numKafkaDatasets,
            numElasticDatasets,
            numPostgresDatasets,
            lensesBaseUrl,
            lensesCreds,
            lensesBasicAuthCreds,
            elasticsearchBaseUrl,
            maybeElasticCreds,
            postgresSchema,
            postgresDatabase,
            postgresCreds
          ) =>
        import io.lenses.data.generator.schema.Gens

        val ec = scala.concurrent.ExecutionContext.global
        implicit val cs: ContextShift[IO] = IO.contextShift(ec)
        val config =
          schema.Gens.Config(recordsOnly = true, maxDepth = 3)

        val maybeTransactor = postgresDatabase.map { db =>
          Transactor.fromDriverManager[IO](
            "org.postgresql.Driver", // driver classname
            s"jdbc:postgresql:$db",
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
              httpClient,
              lensesCreds,
              lensesBasicAuthCreds
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
                              postgresSchema,
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
                        .Kafka(lensesClient)
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
        ).get

        val generator = generators(oldGen.dataSet)
        oldGen.format match {
          case FormatType.AVRO  => generator.avro(oldGen.topic)
          case FormatType.JSON  => generator.json(oldGen.topic)
          case FormatType.XML   => generator.xml(oldGen.topic)
          case FormatType.PROTO => generator.protobuf(oldGen.topic)
        }
    }

}
