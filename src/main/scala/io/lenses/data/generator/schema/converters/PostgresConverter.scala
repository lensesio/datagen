package io.lenses.data.generator.schema.converters

import io.lenses.data.generator.schema.{Record, Schema}
import io.lenses.data.generator.schema
import io.lenses.data.generator.schema.pg._
import io.lenses.data.generator.schema.pg
import io.lenses.data.generator.schema.ArrayOf

class PostgresConverter(config: PostgresConfig) {
  def apply(schema: Schema, schemaName: Option[String]): TableDefinition =
    (schema, schemaName) match {
      case (Record(fields, description), Some(name)) =>
        val pk = if (config.addPrimaryKey) Some("id") else None
        TableDefinition(
          name,
          fields.map(toPg),
          config.containingSchema,
          pk
        )

      case _ =>
        throw new IllegalArgumentException(
          "JDBC converter is not recursive: record and schema name expected"
        )
    }

  private def toPg(sf: schema.Field): Field = {

    def dimensionAndType(
        n: Int,
        value: schema.Schema
    ): (Int, pg.Primitive) =
      value match {
        case schema.Primitive.String  => n -> ColumnType.Text
        case schema.Primitive.Int     => n -> ColumnType.Integer
        case schema.Primitive.Double  => n -> ColumnType.Double
        case schema.Primitive.Boolean => n -> ColumnType.Boolean
        case schema.Record(_, _)      => n -> ColumnType.JSON
        case ArrayOf(schema)          => dimensionAndType(n + 1, schema)

      }

    def columnType(value: schema.Schema): ColumnType =
      value match {
        case schema.Primitive.String  => ColumnType.Text
        case schema.Primitive.Int     => ColumnType.Integer
        case schema.Primitive.Double  => ColumnType.Double
        case schema.Primitive.Boolean => ColumnType.Boolean
        case schema.Record(_, _)      => ColumnType.JSON
        case schema.ArrayOf(v) => {
          val (dimension, tpe) = dimensionAndType(1, v)
          ColumnType.Array(dimension, tpe)
        }
      }
    Field(sf.name, sf.description, columnType(sf.value), sf.isNullable)
  }
}
