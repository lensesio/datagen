package io.lenses.data.generator

import cats.effect.IO
import org.http4s.client.Client
import io.lenses.data.generator.cli.Creds
import org.http4s.Uri
import org.http4s.Method
import org.http4s.circe._
import io.circe.Json
import org.http4s.Request
import org.http4s.headers.Authorization
import org.http4s.BasicCredentials

class Elasticsearch(
    httpClient: Client[IO],
    creds: Option[Creds],
    baseUrl: Uri
) {
  def countIndexes: IO[Int] = {
    val uri = (baseUrl / "_cat" / "indices").withQueryParam("format", "json")
    val request = Request[IO](method = Method.GET, uri = uri)
    val authedRequest = creds.fold(request) {
      case Creds(user, pass) =>
        request.putHeaders(
          Authorization(BasicCredentials(user, pass))
        )

    }
    httpClient.expect[Json](authedRequest).flatMap { json =>
      json.asArray
        .fold(
          IO.raiseError[Int](new Exception(s"JSON array expected, got $json "))
        ) { array =>
          IO.pure(array.size)
        }

    }
  }
}
