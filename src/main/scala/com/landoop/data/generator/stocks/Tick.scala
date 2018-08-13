package com.landoop.data.generator.stocks

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}

import scala.collection.JavaConverters._

case class Exchange(name: String, symbol: String)

object Exchange {
  def forSymbol(symbol: String): Exchange = {
    val name = symbol match {
      case "A" => "NYSE MKT"
      case "N" => "New York Stock Exchange (NYSE)"
      case "P" => "NYSE ARCA"
      case "Z" => "BATS Global Markets (BATS)"
      case "V" => "Investors' Exchange, LLC (IEXG)"
    }
    Exchange(name, symbol)
  }
}

case class Stock(symbol: String, name: String, etf: Boolean, exchange: Exchange, lotSize: Int)

object Stock {

  def load: Seq[Stock] = {

    val settings: CsvParserSettings = new CsvParserSettings
    settings.getFormat.setLineSeparator("\n")
    settings.getFormat.setDelimiter('|')

    val parser: CsvParser = new CsvParser(settings)
    parser.parseAllRecords(getClass.getResourceAsStream("/otherlisted.txt")).asScala
      .map { record =>
        Stock(
          record.getString("NASDAQ Symbol"),
          record.getString("Security Name"),
          record.getBoolean("ETF"),
          Exchange.forSymbol(record.getString("Exchange")),
          record.getInt("Round Lot Size")
        )
      }
  }
}

case class Tick(symbol: String, name: String, category: String, big: Double, ask: Double, etf: Boolean, lotSize: Int, exchange: Exchange)
