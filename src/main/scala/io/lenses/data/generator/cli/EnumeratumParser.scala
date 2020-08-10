package io.lenses.data.generator.cli

import caseapp.core.argparser.ArgParser
import enumeratum._
import caseapp.core.argparser.SimpleArgParser
import caseapp.core.Error
import cats.syntax.either._

trait EnumeratumParser {
  implicit def parseEnum[A <: EnumEntry](implicit
      enum: Enum[A]
  ): ArgParser[A] = {
    val values = enum.values.mkString(", ")
    SimpleArgParser.from[A](values) { name =>
      enum
        .withNameEither(name)
        .leftMap[Error](err =>
          Error.MalformedValue(
            err.notFoundName,
            s"Acceptable values are: $values"
          )
        )
    }
  }

}
