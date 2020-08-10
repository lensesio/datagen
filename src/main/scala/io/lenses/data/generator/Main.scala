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
import cats.instances.stream._
import cats.syntax.traverse._
import org.apache.avro.io.EncoderFactory
import io.lenses.data.generator.schema.AvroConverter
import io.lenses.data.generator.schema.DatasetCreator
import org.http4s.client.middleware.Logger

object Main extends caseapp.CommandApp[Command] with StrictLogging {
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
            lensesBaseUrl,
            lensesCreds,
            elasticsearchBaseUrl
          ) =>
        import org.scalacheck.Gen
        import io.lenses.data.generator.schema.Gens

        val ec = scala.concurrent.ExecutionContext.global
        implicit val cs: ContextShift[IO] = IO.contextShift(ec)
        val config =
          schema.Gens.Config(recordsOnly = true, maxDepth = 3)

        BlazeClientBuilder[IO](ec).resource
          .use { httpClient0 =>
            val httpClient = Logger(true, true)(httpClient0)

            val client = LensesClient(
              lensesBaseUrl,
              httpClient
            )

            fs2
              .Stream(
                Gens
                  .namedSchemas(numKafkaDatasets, config)
                  .parEvalMap(3) {
                    case (datasetName, schema) =>
                      DatasetCreator
                        .Kafka(client, lensesCreds)
                        .create(datasetName, schema)(ec, cs)
                  },
                Gens
                  .namedSchemas(numElasticDatasets, config)
                  .parEvalMap(3) {
                    case (datasetName, schema) =>
                      DatasetCreator
                        .Elasticsearch(
                          httpClient,
                          elasticsearchBaseUrl
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
