package com.landoop.data.generator

import java.sql.DriverManager

import com.landoop.data.generator.stocks.StockGenerator
import com.typesafe.scalalogging.StrictLogging
import scopt.OptionParser

object JdbcGenerator extends App with StrictLogging {

  case class JdbcConfig(url: String, count: Int)

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

  val parser: OptionParser[JdbcConfig] = new scopt.OptionParser[JdbcConfig]("generator") {
    head("generator")

    opt[String]("url").required().action { case (url, a) =>
      a.copy(url = url)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid url") else Right(()))
      .text("url")

    opt[Int]("count").optional().action { case (count, a) =>
      a.copy(count = count)
    }.validate(f => Right(f))
      .text("count")
  }

  parser.parse(args, JdbcConfig("", 1000)).foreach { config =>

    val conn = DriverManager.getConnection(config.url)
    conn.createStatement().execute("CREATE TABLE IF NOT EXISTS stockticks (category varchar(10), etf bool, symbol varchar(8), name varchar(255), bid double, ask double, lotsize smallint)")

    val stmt = conn.prepareStatement("INSERT INTO stockticks (category, etf, symbol, name, bid, ask, lotsize) VALUES (?,?,?,?,?,?,?)")
    for (k <- 1 to config.count) {
      val tick = StockGenerator.generateTick
      stmt.setString(1, tick.category)
      stmt.setBoolean(2, tick.etf)
      stmt.setString(3, tick.symbol)
      stmt.setString(4, tick.name)
      stmt.setDouble(5, tick.bid)
      stmt.setDouble(6, tick.ask)
      stmt.setInt(7, tick.lotSize)
      stmt.addBatch()
      if (k % 1000 == 0) {
        stmt.executeBatch()
        stmt.clearBatch()
        println(s"Completed $k messsages")
      }
    }

    if (config.count % 1000 > 0) {
      stmt.executeBatch()
    }

    stmt.executeBatch()
    conn.close()
  }
}
