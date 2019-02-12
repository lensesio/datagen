package io.lenses.datagen.redis

import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import java.util.*

data class Stock(val symbol: String, val name: String, val etf: Boolean, val exchange: Exchange, val lotSize: Int) {
  companion object {

    val stocks: Array<Stock> = CsvParserSettings().run {
      format.setLineSeparator("\n")
      format.delimiter = '|'
      isHeaderExtractionEnabled = true

      val parser = CsvParser(this)
      parser.parseAllRecords(this::class.java.getResourceAsStream("/otherlisted.txt")).map {
        Stock(
            it.getString("NASDAQ Symbol"),
            it.getString("Security Name"),
            it.getString("ETF") == "Y",
            Exchange.forSymbol(it.getString("Exchange")),
            it.getInt("Round Lot Size")
        )
      }.toTypedArray()
    }
  }
}

data class Exchange(val name: String, val symbol: String) {
  companion object {
    fun forSymbol(symbol: String): Exchange {
      val name = when (symbol) {
        "A" -> "NYSE MKT"
        "N" -> "New York Stock Exchange (NYSE)"
        "P" -> "NYSE ARCA"
        "Z" -> "BATS Global Markets (BATS)"
        "V" -> "Investors' Exchange, LLC (IEXG)"
        else -> "Nasdaq"
      }
      return Exchange(name, symbol)
    }
  }
}

data class Tick(val symbol: String, val name: String, val category: String, val bid: Double, val ask: Double, val etf: Boolean, val lotSize: Int, val exchange: Exchange) {

  fun toMap(): Map<String, String> {
    return mapOf(
        "symbol" to symbol,
        "name" to name,
        "category" to category,
        "bid" to bid.toString(),
        "ask" to ask.toString(),
        "etf" to etf.toString(),
        "lotSize" to lotSize.toString(),
        "exchange.name" to exchange.name,
        "exchange.symbol" to exchange.symbol
    )
  }

  companion object {
    private val random = Random()
    fun gen(): Tick {
      val stock = Stock.stocks[random.nextInt(Stock.stocks.size)]
      val price = random.nextDouble() * 1000
      val spread = price / random.nextDouble()
      val bid = price - spread
      val ask = price + spread
      return Tick(
          stock.symbol,
          stock.name,
          listOf("N", "Q").shuffled().first(),
          bid,
          ask,
          stock.etf,
          random.nextInt(100) * 100,
          stock.exchange
      )
    }
  }
}