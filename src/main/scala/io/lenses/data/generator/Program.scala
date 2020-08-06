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

object Program extends caseapp.CaseApp[Arguments] with StrictLogging {
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

  def run(arguments: Arguments, otherArgs: RemainingArgs) = {
    logger.info(
      """
      |
      |  _
      | | |    ___ _ __  ___  ___  ___
      | | |   / _ \ '_ \/ __|/ _ \/ __|
      | | |__|  __/ | | \__ \  __/\__ \
      | |_____\___|_|_|_|___/\___||___/                         _
      | |  _ \  __ _| |_ __ _   / ___| ___ _ __   ___ _ __ __ _| |_ ___  _ __
      | | | | |/ _` | __/ _` | | |  _ / _ \ '_ \ / _ \ '__/ _` | __/ _ \| '__|
      | | |_| | (_| | || (_| | | |_| |  __/ | | |  __/ | | (_| | || (_) | |
      | |____/ \__,_|\__\__,_|  \____|\___|_| |_|\___|_|  \__,_|\__\___/|_|
      |
    """.stripMargin
    )

    implicit val generatorConfig: DataGeneratorConfig = DataGeneratorConfig(
      arguments.brokers,
      arguments.schema,
      arguments.produceDelay,
      arguments.schemaMode
    )

    CreateTopicFn(
      arguments
    )

    val generator = generators(arguments.dataSet)
    arguments.format match {
      case FormatType.AVRO  => generator.avro(arguments.topic)
      case FormatType.JSON  => generator.json(arguments.topic)
      case FormatType.XML   => generator.xml(arguments.topic)
      case FormatType.PROTO => generator.protobuf(arguments.topic)
    }
  }

}
