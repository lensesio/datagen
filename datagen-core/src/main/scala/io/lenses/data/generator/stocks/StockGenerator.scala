package io.lenses.data.generator.stocks

import scala.util.Random

object StockGenerator {
  def generateTick: Tick = {
    val stock = Stock.stocks(Random.nextInt(Stock.stocks.length))
    val price = Random.nextDouble() * 1000
    val spread = price / Random.nextDouble()
    val bid = price - spread
    val ask = price + spread
    Tick(
      stock.symbol,
      stock.name,
      Random.shuffle(List("N", "Q")).head,
      bid,
      ask,
      stock.etf,
      Random.nextInt(100) * 100,
      stock.exchange
    )
  }
}
