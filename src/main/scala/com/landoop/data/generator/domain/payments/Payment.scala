package com.landoop.data.generator.domain.payments

case class Payment(id: String, time: String, amount: BigDecimal, currency: String, creditCardId: String, merchantId: Long)

object Payment{
  val Topic = "payment"
}