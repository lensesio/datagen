package io.lenses.data.generator.schema

import org.scalacheck.Gen
import cats.data.NonEmptyList
import scala.annotation.meta.field
import scala.jdk.CollectionConverters._
import java.{util => ju}

object Gens {

  private lazy val nouns: Vector[String] =
    scala.io.Source.fromResource("nounlist.txt").getLines().toVector

  def primitive: Gen[Schema] =
    Gen.oneOf(
      List(
        Primitive.Boolean,
        Primitive.Double,
        Primitive.Int,
        Primitive.String
      )
    )

  def array(gen: Gen[Schema] = primitive): Gen[Schema] =
    gen.map(ArrayOf(_))

  def noun = Gen.oneOf(nouns).map(_.replace(" ", "").replace("-", "_"))
  def compositeNoun(maxWords: Int = 8, camelCase: Boolean): Gen[String] =
    Gen.chooseNum(1, maxWords).flatMap { numWords =>
      Gen.sequence(List.fill(numWords)(noun)).map { nouns =>
        if (camelCase)
          nouns.asScala.zipWithIndex.map {
            case (s, idx) =>
              if (idx > 0) s.capitalize
              else s
          }.mkString
        else
          nouns.asScala.mkString("_")
      }
    }

  def field(
      name: String,
      gen: Gen[Schema] = primitive,
      nullable: Gen[Boolean] = Gen.oneOf[Boolean](true, false)
  ): Gen[Field] =
    for {
      tpe <- gen
      isNullable <- nullable
    } yield Field(name, tpe, isNullable, None)

  def record(
      camelCase: Boolean,
      maxFields: Int = 15,
      fieldValueGen: Gen[Schema] = Gen.oneOf(primitive, array())
  ): Gen[Schema] =
    for {
      numFields <- Gen.choose(0, maxFields)
      fieldNames <-
        Gen.containerOfN[Set, String](numFields, compositeNoun(8, camelCase))
      fieldGens = fieldNames.map(name => field(name, fieldValueGen))
      fields <- Gen.sequence(fieldGens).map(_.asScala.toVector.toList)

    } yield Record(fields, None)

  def schema: Gen[Schema] =
    Gen.oneOf[Boolean](true, false).flatMap { camelCase =>
      record(camelCase)
    }

  def namedSchema: Gen[(String, Schema)] =
    for {
      name <- compositeNoun(maxWords = 4, camelCase = true)
      uuid = ju.UUID.randomUUID().toString().replace("-", "_")
      schema <- Gens.schema

    } yield s"$name${uuid}" -> schema
}
