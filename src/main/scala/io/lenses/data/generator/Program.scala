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

object Program extends caseapp.CommandApp[Command] with StrictLogging {
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
      case NewGen(numDatasets) =>
        import org.scalacheck.Gen
        import io.lenses.data.generator.schema.Gens

        val ec = scala.concurrent.ExecutionContext.global
        implicit val cs: ContextShift[IO] = IO.contextShift(ec)

        BlazeClientBuilder[IO](ec).resource
          .use { httpClient =>
            val client = LensesClient(
              Uri.unsafeFromString("http://localhost:24015"),
              httpClient
            )

            client.login("admin", "admin").flatMap { implicit auth =>
              Gen.infiniteStream(Gens.namedSchema).sample match {
                case None =>
                  IO.raiseError(
                    new IllegalStateException("Cannot generate data")
                  )
                case Some(schemas) =>
                  schemas.take(numDatasets).traverse {
                    case (schemaName, schema) =>
                      client.createTopic(schemaName) *> client.setTopicMetadata(
                        schemaName,
                        AvroConverter(schema, Some(schemaName))
                      )
                  }
              }
            }
          }
          .unsafeRunSync()

        def printFields(schema: Schema) = {
          println("====================")
          schema match {
            case Record(fields, _) =>
              fields.foreach(field => println(field.name))
            case _ => println("not a record ...")
          }
        }

      case oldGen: OldGen =>
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
