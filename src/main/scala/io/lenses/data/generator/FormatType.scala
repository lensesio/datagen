package io.lenses.data.generator

import enumeratum._

sealed trait FormatType extends EnumEntry
object FormatType extends Enum[FormatType] {
  case object AVRO extends FormatType
  case object JSON extends FormatType
  case object XML extends FormatType
  case object PROTO extends FormatType

  override val values = findValues
}
