package io.lenses.data.generator

import io.lenses.data.generator.stocks.StockGenerator
import com.typesafe.scalalogging.StrictLogging
import javax.jms.Session
import org.apache.activemq.ActiveMQConnectionFactory
import scopt.OptionParser

object JmsGenerator extends App with StrictLogging {

  case class JmsConfig(url: String, queueName: String, count: Int)

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

    opt[Int]("count").optional().action { case (count, a) =>
      a.copy(count = count)
    }.validate(f => Right(f))
      .text("count")
  }

  parser.parse(args, JmsConfig("", "", 1000)).foreach { config =>

    val factory = new ActiveMQConnectionFactory(config.url)
    val conn = factory.createConnection()
    conn.start()
    val sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue = sess.createQueue(config.queueName)

    val producer = sess.createProducer(queue)

    for (k <- 1 to config.count) {
      val tick = StockGenerator.generateTick
      val json =
        s"""{"category": "${tick.category}",
           |"etc": ${tick.etf},
           |"symbol": "${tick.symbol}",
           |"name": "${tick.name}",
           |"bid": ${tick.bid},
           |"ask": ${tick.ask},
           |"lotsize": ${tick.lotSize}
           |}
        """.stripMargin
      val msg = sess.createTextMessage(json)
      producer.send(msg)
      if (k % 1000 == 0)
        println(s"Completed $k messsages")
    }

    producer.close()
    sess.close()
    conn.close()
  }
}
