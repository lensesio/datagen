package com.landoop.data.generator

import com.landoop.data.generator.stocks.StockGenerator
import com.typesafe.scalalogging.StrictLogging
import javax.jms.Session
import org.apache.activemq.ActiveMQConnectionFactory
import scopt.OptionParser

object JmsGenerator extends App with StrictLogging {

  case class JmsConfig(url: String, queueName: String)

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

  val parser: OptionParser[JmsConfig] = new scopt.OptionParser[JmsConfig]("generator") {
    head("generator")

    opt[String]("url").required().action { case (url, a) =>
      a.copy(url = url)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid url") else Right(()))
      .text("url")

    opt[String]("queue").required().action { case (queue, a) =>
      a.copy(queueName = queue)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid queue") else Right(()))
      .text("queue")
  }

  parser.parse(args, JmsConfig("", "")).foreach { config =>

    val factory = new ActiveMQConnectionFactory(config.url)
    val conn = factory.createConnection()
    conn.start()
    val sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue = sess.createQueue(config.queueName)

    val producer = sess.createProducer(queue)

    for (_ <- 1 to 10000) {
      val tick = StockGenerator.generateTick
      val json =
        s"""{"category" -> "${tick.category}",
           |"etc" -> ${tick.etf},
           |"symbol" -> "${tick.symbol}",
           |"name" -> "${tick.name}",
           |"bid" -> ${tick.bid},
           |"ask" -> ${tick.ask},
           |"lotsize" : ${tick.lotSize}
           |}
        """.stripMargin
      val msg = sess.createTextMessage(json)
      producer.send(msg)
    }

    producer.close()
    sess.close()
    conn.close()
  }
}
