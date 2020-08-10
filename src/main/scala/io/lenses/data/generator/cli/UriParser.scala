package io.lenses.data.generator.cli

import caseapp.core.argparser.ArgParser
import caseapp.core.Error
import caseapp.core.argparser.SimpleArgParser
import org.http4s.Uri

trait UriParser {
  implicit val uriParser: ArgParser[Uri] =
    SimpleArgParser.from("URI") { s =>
      Uri.fromString(s).toOption match {
        case None      => Left(Error.MalformedValue(s, "Cannot parse Uri"))
        case Some(uri) => Right(uri)
      }
    }

}
