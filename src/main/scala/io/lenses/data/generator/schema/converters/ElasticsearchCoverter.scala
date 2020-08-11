package io.lenses.data.generator.schema

import io.circe.Json

object ElasticsearchCoverter {
  def apply(schema: Schema, schemaName: Option[String]): Json =
    schema match {
      case Primitive.String =>
        Json.obj("type" -> Json.fromString("text"))
      case Primitive.Int =>
        Json.obj("type" -> Json.fromString("integer"))
      case Primitive.Double =>
        Json.obj("type" -> Json.fromString("double"))
      case Primitive.Boolean =>
        Json.obj("type" -> Json.fromString("boolean"))
      case ArrayOf(schema) =>
        apply(schema, None)
      case Record(fields, _) =>
        Json.obj(
          "properties" -> Json.obj(
            fields.map { f =>
              f.name -> apply(f.value, None)
            }: _*
          )
        )

    }
}
