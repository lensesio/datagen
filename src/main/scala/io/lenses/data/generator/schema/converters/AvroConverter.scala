package io.lenses.data.generator.schema

import org.apache.avro.SchemaBuilder
import org.apache.avro.SchemaBuilder.TypeBuilder
import org.apache.avro
import org.apache.avro.SchemaBuilder.BaseTypeBuilder
import org.apache.avro.SchemaBuilder.UnionFieldTypeBuilder

object AvroConverter {
  def apply(schema: Schema, schemaName: Option[String]): avro.Schema =
    (schema, schemaName) match {
      case (Primitive.String, _)  => SchemaBuilder.builder.stringType()
      case (Primitive.Int, _)     => SchemaBuilder.builder.intType()
      case (Primitive.Double, _)  => SchemaBuilder.builder.doubleType()
      case (Primitive.Boolean, _) => SchemaBuilder.builder.booleanType()
      case (ArrayOf(schema), _) =>
        SchemaBuilder.array().items(AvroConverter(schema, None))
      case (Record(fields, description), Some(name)) =>
        val baseRecord = SchemaBuilder.record(name)
        fields
          .foldLeft(description.map(baseRecord.doc).getOrElse(baseRecord).fields()) {
            case (b, field) =>
              val valueSchema0 =
                AvroConverter(field.value, Some(s"${name}_${field.name}"))

              val valueSchema =
                if (field.isNullable)
                  SchemaBuilder.unionOf
                    .nullType()
                    .and()
                    .`type`(valueSchema0)
                    .endUnion()
                else
                  valueSchema0

              val named = b.name(field.name)
              field.description.map(named.doc).getOrElse(named).`type`(valueSchema)
                .noDefault()

          }
          .endRecord()
      case (Record(_, _), None) =>
        throw new IllegalArgumentException("Schema name expected for records")

    }
}
