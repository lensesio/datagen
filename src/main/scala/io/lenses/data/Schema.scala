package io.lenses.data

sealed trait Schema

trait Primitive extends Schema
object Primitive {
  case object Int extends Primitive
  case object Double extends Primitive
  case object String extends Primitive
  case object Boolean extends Primitive
  case object Null extends Primitive
}
final case class ArrayOf(schema: Schema)
final case class Field(name: String, value: Schema, isNullable: Boolean)
final case class Record(fields: List[Field]) extends Schema
