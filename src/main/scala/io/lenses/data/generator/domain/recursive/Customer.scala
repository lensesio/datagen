package io.lenses.data.generator.domain.recursive

import com.sksamuel.avro4s.RecordFormat

case class Customer(name: String, policyId: String, dependant:Option[Customer])

object Customer {
  implicit val recordFormat: RecordFormat[Customer] = RecordFormat[Customer]
}