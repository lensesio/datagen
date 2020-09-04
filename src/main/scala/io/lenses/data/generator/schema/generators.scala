package io.lenses.data.generator.schema

import org.scalacheck.Gen
import cats.data.NonEmptyList
import scala.annotation.meta.field
import scala.jdk.CollectionConverters._
import java.{util => ju}
import cats.effect.IO
import cats.effect.ContextShift
import org.http4s.client.blaze.BlazeClientBuilder
import io.lenses.data.generator.http.LensesClient
import scala.concurrent.ExecutionContext
import cats.syntax.traverse._
import cats.instances.stream._
import org.http4s.Uri
import fs2.Stream

object Gens {

  final case class Config(
      recordsOnly: Boolean,
      maxDepth: Int = 3
  )

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
      documentation <- Gen.option(Gen.listOfN(10, noun).map(_.mkString(" ")))
    } yield Field(name, tpe, isNullable, documentation)

  def record(
      maxFields: Int = 15,
      depth: Int,
      camelCase: Boolean,
      fieldValueGen: Option[Gen[Schema]]
  )(implicit config: Config): Gen[Schema] = {
    println(s"generating record depth: ${depth}...")
    def valueGen =
      fieldValueGen.getOrElse {
        if (depth < config.maxDepth)
          Gen.oneOf(
            record(maxFields, depth + 1, camelCase, None),
            primitive,
            array(primitive)
          ) //TODO: array of record?
        else Gen.oneOf(primitive, array(primitive))
      }
    for {
      numFields <- Gen.choose(1, maxFields)
      fieldNames <-
        Gen.containerOfN[Set, String](numFields, compositeNoun(8, camelCase))
      fieldGens = fieldNames.map(name => field(name, valueGen))
      fields <- Gen.sequence(fieldGens).map(_.asScala.toVector.toList)
      documentation <- Gen.option(Gen.listOfN(10, noun).map(_.mkString(" ")))
    } yield Record(fields, documentation)
  }

  def schema(
      fieldValueGen: Option[Gen[Schema]] = None
  )(implicit config: Config): Gen[Schema] =
    Gen.oneOf[Boolean](true, false).flatMap { camelCase =>
      record(15, 1, camelCase, fieldValueGen)
    }

  def namedSchema(implicit config: Config): Gen[(String, Schema)] =
    for {
      name <- compositeNoun(maxWords = 4, camelCase = true)
      id = scala.util.Random.nextInt(100000)
      schema <- Gens.schema()

    } yield s"$name${id}" -> schema

  //config is passed in explicitly as it will slightly vary based on the source data system
  //(e.g. )
  def namedSchemas(size: Int, config: Config): Stream[IO, (String, Schema)] = {
    implicit val config0 = config
    Gen.infiniteStream(namedSchema).sample match {
      case None =>
        Stream.raiseError[IO](
          new IllegalStateException("Couldn't generate testdata for schema")
        )
      case Some(infiniteStream) => Stream.emits(infiniteStream.take(size))
    }
  }
}
