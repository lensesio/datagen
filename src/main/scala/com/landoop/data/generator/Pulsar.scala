package com.landoop.data.generator

import com.landoop.data.generator.stocks.{StockGenerator, Tick}
import com.sksamuel.pulsar4s.{ProducerConfig, PulsarClient, Topic}
import com.typesafe.scalalogging.StrictLogging
import scopt.OptionParser

object Pulsar extends App with StrictLogging {

  case class PulsarConfig(topic: String,
                          url: String)

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

  val parser: OptionParser[PulsarConfig] = new scopt.OptionParser[PulsarConfig]("generator") {
    head("generator")

    opt[String]("topic").required().action { case (topic, a) =>
      a.copy(topic = topic)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid topic") else Right(()))
      .text("Pulsar topic to publish to")

    opt[String]("url").required().action { case (url, a) =>
      a.copy(url = url)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid url") else Right(()))
      .text("Pulsar client URL")
  }

  parser.parse(args, PulsarConfig("", "")).foreach { config =>

    import com.sksamuel.pulsar4s.circe._
    import io.circe.generic.auto._

    val client = PulsarClient(config.url)
    val topic = Topic(config.topic)
    val producer = client.producer[Tick](ProducerConfig(topic))

    for (_ <- 1 to 100000) {
      producer.send(StockGenerator.generateTick)
    }

    producer.close()
  }
}
