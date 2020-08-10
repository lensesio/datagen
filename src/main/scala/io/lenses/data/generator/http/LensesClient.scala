package io.lenses.data.generator.http

import cats.effect.IO
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.Request
import org.http4s.circe._
import io.circe.Json
import org.http4s.Method
import org.http4s.Headers
import org.http4s.Header
import org.apache.avro
import org.apache.avro.Schema
import io.lenses.data.generator.cli.LensesCreds

object LensesClient {
  final case class AuthToken(value: String) extends AnyVal

  def apply(
      baseUrl: Uri,
      client: Client[IO]
  ): LensesClient =
    new LensesClient {

      override def setTopicMetadata(topicName: String, schema: Schema)(implicit
          auth: AuthToken
      ) = {
        val request =
          Request[IO](
            method = Method.POST,
            uri = baseUrl / "api" / "v1" / "metadata" / "topics"
          )
            .withHeaders(
              Header("X-Kafka-Lenses-Token", auth.value)
            )
            .withEntity(
              Json.obj(
                "topicName" -> Json.fromString(topicName),
                "keyType" -> Json.fromString("BYTES"),
                "valueType" -> Json.fromString("JSON"),
                "valueSchema" -> Json.fromString(schema.toString(true))
              )
            )
        client.expect[Unit](request)

      }

      override def login(username: String, password: String) = {
        val request =
          Request[IO](method = Method.POST, uri = baseUrl / "api" / "login")
            .withEntity(
              Json.obj(
                "user" -> Json.fromString(username),
                "password" -> Json.fromString(password)
              )
            )
        client.expect[String](request).map(AuthToken(_))
      }

      override def createTopic(topicName: String)(implicit
          auth: AuthToken
      ): IO[Unit] = {
        val entity = Json.obj(
          "topicName" -> Json.fromString(topicName),
          "replication" -> Json.fromInt(1),
          "partitions" -> Json.fromInt(1),
          "configs" -> Json.obj(
            "cleanup.policy" -> Json.fromString("compact")
          )
        )
        val request =
          Request[IO](method = Method.POST, uri = baseUrl / "api" / "topics")
            .withHeaders(
              Header("X-Kafka-Lenses-Token", auth.value)
            )
            .withEntity(entity)
        client.expect[Unit](request)
      }
    }
}

import LensesClient.AuthToken
trait LensesClient {
  def login(username: String, password: String): IO[AuthToken]
  def createTopic(topicName: String)(implicit auth: AuthToken): IO[Unit]
  def setTopicMetadata(topicName: String, schema: avro.Schema)(implicit
      auth: AuthToken
  ): IO[Unit]
}
