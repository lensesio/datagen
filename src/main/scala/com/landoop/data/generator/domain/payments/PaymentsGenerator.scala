package com.landoop.data.generator.domain.payments

import com.landoop.data.generator.domain.DataGenerator
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import scala.math.BigDecimal.RoundingMode
import scala.util.Random
import  DataGenerator.instantFormat
object PaymentsGenerator extends DataGenerator[Payment] {
  private val MerchantIds = (1 to 100).map(_.toLong).toVector
  private val DateFormatter = ISODateTimeFormat.dateTime()

  override protected def generate(): Seq[(String, Payment)] = {
    val index = Random.nextInt(CreditCard.Cards.size)
    val cc = CreditCard.Cards(index)

    val dt = new DateTime().toDateTime(DateTimeZone.UTC)
    val date = DateFormatter.print(dt)

    val left = 10 + Random.nextInt(5000)
    val right = Random.nextInt(100)
    val decimal = BigDecimal(s"$left.$right").setScale(18, RoundingMode.HALF_UP)
    val id = s"txn${System.currentTimeMillis()}"
    List(
      id -> Payment(id, date, decimal, cc.currency, cc.number, MerchantIds(Random.nextInt(MerchantIds.size)))
    )
  }
}
