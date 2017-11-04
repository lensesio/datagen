package com.landoop.data.generator

import java.io.File

import com.landoop.data.generator.config.ConfigExtension._
import com.landoop.data.generator.config.DataGeneratorConfig
import com.landoop.data.generator.domain.Generator
import com.landoop.data.generator.domain.payments.{CreditCardGenerator, PaymentsGenerator}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.util.{Failure, Success, Try}

object Program extends App with StrictLogging {
  private val arguments =
    """
      |Data Generator Arguments
      |$configurationFile  $topic $option [$pauseBetweenRecords]
      |
      |Available options:
      | 1 -  credit card data
      | 2 -  payments data
    """.stripMargin

  val generators = Map[Int, Generator](
    1 -> CreditCardGenerator,
    2 -> PaymentsGenerator
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

  checkAndExit(args.nonEmpty, "Missing application configuration file argument.")
  checkAndExit(args.length > 1, "Missing target topic argument")
  val topic = args(1)

  checkAndExit(args.length > 2, "Missing option argument.")
  val maybeOption = Try(args(2).toInt).toOption
  checkAndExit(maybeOption.isDefined, s"Invalid option value. Available values are: ${generators.keys.mkString(",")}")

  val maybeGenerator = generators.get(maybeOption.get)
  checkAndExit(maybeGenerator.isDefined, s"Invalid option value. Available values are: ${generators.keys.mkString(",")}")

  var pauseBetweenRecords = 100L
  if (args.length == 4) {
    val maybePause = Try(args(3).toLong).toOption
    checkAndExit(maybePause.isDefined, s"Invalid pause value.")
    checkAndExit(maybePause.get >= 0, s"Invalid pause value.")
    pauseBetweenRecords = maybePause.get
  }
  val generator = maybeGenerator.get

  val configFile = new File(args(0))
  checkAndExit(configFile.exists(), s"Invalid configuration file. $configFile can not be found")
  val config = ConfigFactory.parseFile(new File(args(0)))
  val brokers = config.readString("brokers", null)

  checkAndExit(brokers != null, "Missing 'brokers' configuration entry.")
  val schemaRegistry = config.readString("schema.registry", null)
  checkAndExit(schemaRegistry != null, "Missing 'schema.registry' configuration entry.")

  val formatValue = config.readString("format", null)
  checkAndExit(formatValue != null, "Missing 'format' configuration entry")

  val format: FormatType = Try {
    FormatType.valueOf(formatValue.trim.toUpperCase())
  } match {
    case Failure(t) =>
      logger.info(s"Format '$formatValue' is incorrect. Expecting: Avro,Json. Application will exit...")
      sys.exit(1000)
    case Success(f) => f
  }

  implicit val generatorConfig = DataGeneratorConfig(brokers, schemaRegistry, pauseBetweenRecords)

  format match {
    case FormatType.AVRO => generator.avro(topic)
    case FormatType.JSON => generator.json(topic)
  }


  def checkAndExit(thunk: => Boolean, msg: String): Unit = {
    val value = thunk
    if (!value) {
      logger.info(msg)
      logger.info("The application will close...")
      logger.info(arguments)
      sys.exit(1)
    }
  }

}
