package io.lenses.data.generator.schema

sealed trait Schema
sealed trait Primitive extends Schema
object Primitive {
  case object Int extends Primitive
  case object Double extends Primitive
  case object String extends Primitive
  case object Boolean extends Primitive
}
final case class ArrayOf(schema: Schema) extends Schema
final case class Field(
    name: String,
    value: Schema,
    isNullable: Boolean,
    description: Option[String]
)

final case class Record(fields: List[Field], description: Option[String])
    extends Schema