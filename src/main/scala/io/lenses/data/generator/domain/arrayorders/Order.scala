package io.lenses.data.generator.domain.arrayorders

import com.sksamuel.avro4s.RecordFormat

case class Order(shipment_no:String, orderno:String, psp_order_no:String, omnihubinstance:String)

object Order {
  implicit val recordFormat: RecordFormat[Order] = RecordFormat[Order]
}