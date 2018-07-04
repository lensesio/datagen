package com.landoop.data.generator

import java.net.URL

import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.domain.Generator
import com.landoop.data.generator.domain.iot.{DeviceTemperatureArrayDataGenerator, DeviceTemperatureDataGenerator, SensorDataGenerator}
import com.landoop.data.generator.domain.payments.{CreditCardGenerator, PaymentsGenerator}
import com.landoop.data.generator.domain.weather.WeatherDataGenerator
import com.typesafe.scalalogging.StrictLogging
import scopt.Read

import scala.util.Try

object Program extends App with StrictLogging {
  val generators = Map[Int, Generator](
    1 -> CreditCardGenerator,
    2 -> PaymentsGenerator,
    3 -> SensorDataGenerator,
    4 -> WeatherDataGenerator,
    5 -> DeviceTemperatureDataGenerator,
    6 -> DeviceTemperatureArrayDataGenerator
  )

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
    """.stripMargin)

  val parser = new scopt.OptionParser[Arguments]("generator") {
    head("generator")

    opt[String]("topic").required().action { case (b, a) =>
      a.copy(topic = b)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid topic") else Right(()))
      .text("Kafka topic to publish to")

    opt[Int]("partitions").action { case (b, a) =>
      a.copy(partitions = b)
    }.text("Topic partitions")

    opt[Int]("replication").action { case (b, a) =>
      a.copy(replication = b)
    }.text("Topic replication")

    opt[String]("brokers").required().action { case (b, a) =>
      a.copy(brokers = b)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid Kafka bootstrap brokers") else Right(()))
      .text("Kafka bootstrap brokers")

    opt[String]("schema").action { case (v, a) =>
      a.copy(schemaRegistry = v)
    }.validate(f => if (f.trim.nonEmpty && Try(new URL(f.trim)).isSuccess) Right(()) else Left("Invalid Schema Registry URL"))
      .text("Schema Registry URL")

    opt[String]("schema.mode").action { case (v, a) =>
      val mode = v.trim.toLowerCase()
      a.copy(defaultSchemaRegistry = mode == "hortonworks" || mode == "hw")
    }.text("Schema registry mode. Use 'hw/hortonworks' to use the hortonworks serializers")

    implicit val evFormatType: Read[FormatType] = new scopt.Read[FormatType] {
      override def arity: Int = 1

      override def reads: String => FormatType = a => FormatType.valueOf(a.trim.toUpperCase())
    }
    opt[FormatType]("format").required().action { case (v, a) =>
      a.copy(format = v)
    }.text("Format type. AVRO/JSON/XML")

    opt[Int]("data").required().action((v, a) => a.copy(dataSet = v))
      .validate(v => if (v < 0 && v > 5) Left("Invalid data set option. Available options are 1 to 5") else Right(()))
      .text(
        """
          |Data set type.
          |Available options:
          | 1 -  credit card data
          | 2 -  payments data
          | 3 -  sensor data
          | 4 -  weather data
          | 5 -  device temperature
          | 6 -  device temperature array""".stripMargin)
  }

  parser.parse(args, Arguments("", 1, 1, -1, FormatType.JSON, "", "")).foreach { implicit arguments =>
    implicit val generatorConfig: DataGeneratorConfig = DataGeneratorConfig(arguments.brokers, arguments.schemaRegistry, arguments.produceDelay, arguments.defaultSchemaRegistry)

    CreateTopicFn(arguments.topic, arguments.partitions, arguments.replication)
    val generator = generators(arguments.dataSet)
    arguments.format match {
      case FormatType.AVRO => generator.avro(arguments.topic)
      case FormatType.JSON => generator.json(arguments.topic)
      case FormatType.XML => generator.xml(arguments.topic)
      case FormatType.PROTO => generator.protobuf(arguments.topic)
    }
  }

}
