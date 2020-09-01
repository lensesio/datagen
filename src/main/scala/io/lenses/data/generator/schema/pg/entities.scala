package io.lenses.data.generator.schema.pg

import doobie._
import doobie.implicits._
import doobie.util.fragments
import cats.implicits._

case class PostgresConfig(
    containingSchema: Option[String],
    addPrimaryKey: Boolean
)

sealed trait Primitive {
  def asDLL: Fragment
}
sealed trait ColumnType {
  def asDLL: Fragment
}
object ColumnType {
  case object Text extends ColumnType with Primitive {
    override val asDLL = fr"TEXT"
  }
  case object Integer extends ColumnType with Primitive {

    override val asDLL = fr"INTEGER"

  }
  case object Double extends ColumnType with Primitive {

    override val asDLL = fr"DOUBLE PRECISION"

  }
  case object Boolean extends ColumnType with Primitive {

    override val asDLL = fr"BOOLEAN"

  }
  case object JSON extends ColumnType with Primitive {

    override val asDLL = fr"JSON"

  }
  case class Array(dimension: Int, primitive: Primitive) extends ColumnType {

    override def asDLL: Fragment =
      primitive.asDLL ++ Fragment.const0(List.fill(dimension)("[]").mkString)

  }
}

final case class Field(
    name: String,
    description: Option[String],
    columnType: ColumnType,
    nullable: Boolean
) {
  def asDLL: Fragment =
    Fragment.const0(name) ++ fr" " ++ columnType.asDLL ++ (if (nullable) fr""
                                                           else fr" NOT NULL")
}

final case class TableDefinition(
    name: String,
    fields: List[Field],
    schema: Option[String],
    pk: Option[String]
) {

  def asDLL: Fragment = {
    val tableName = Fragment.const0(name)
    val nameWithSchema =
      schema.fold(tableName)(schema => Fragment.const0(s"${schema}.${name}"))
    val pkColumn =
      pk.map(pk => List(Fragment.const0(pk) ++ fr" SERIAL")).getOrElse(Nil)
    val columns = (pkColumn ++ fields.map(_.asDLL)).intercalate(fr", ")

    fr"CREATE TABLE" ++ nameWithSchema ++ fragments.parentheses(columns)
  }

}
