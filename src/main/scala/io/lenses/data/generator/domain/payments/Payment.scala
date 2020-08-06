package io.lenses.data.generator.domain.payments

import com.sksamuel.avro4s.RecordFormat

case class Payment(id: String,
                   time: String,
                   amount: BigDecimal,
                   currency: String,
                   creditCardId: String,
                   merchantId: Long)

object Payment {
  implicit val recordFormat: RecordFormat[Payment] = RecordFormat[Payment]
}
