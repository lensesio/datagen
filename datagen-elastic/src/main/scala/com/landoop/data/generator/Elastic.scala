package com.landoop.data.generator

import com.landoop.data.generator.stocks.StockGenerator
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticNodeEndpoint, ElasticProperties}
import com.typesafe.scalalogging.StrictLogging
import scopt.OptionParser

object Elastic extends App with StrictLogging {

  case class ElasticConfig(protocol: String,
                           host: String,
                           port: Int,
                           index: String)

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

  val parser: OptionParser[ElasticConfig] = new scopt.OptionParser[ElasticConfig]("generator") {
    head("generator")

    opt[String]("host").required().action { case (host, a) =>
      a.copy(host = host)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid host") else Right(()))
      .text("host")

    opt[Int]("port").required().action { case (port, a) =>
      a.copy(port = port.toInt)
    }.text("port")

    opt[String]("index").required().action { case (index, a) =>
      a.copy(index = index)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid index") else Right(()))
      .text("index")

    opt[String]("protocol").required().action { case (protocol, a) =>
      a.copy(protocol = protocol)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid protocol") else Right(()))
      .text("protocolUrl")
  }

  parser.parse(args, ElasticConfig("", "", 80, "")).foreach { config =>

    import com.sksamuel.elastic4s.http.ElasticDsl._

    val endpoint = ElasticNodeEndpoint(config.protocol, config.host, config.port, None)
    val client = ElasticClient(ElasticProperties(Seq(endpoint)))

    for (_ <- 1 to 10000) {
      val tick = StockGenerator.generateTick
      client.execute {
        index(config.index, "type").fields(Map("category" -> tick.category, "etc" -> tick.etf, "symbol" -> tick.symbol, "name" -> tick.name, "bid" -> tick.bid, "ask" -> tick.ask, "lotsize" -> tick.lotSize))
      }
    }

    client.close()
  }
}
