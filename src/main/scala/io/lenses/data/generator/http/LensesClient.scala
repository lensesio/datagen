package io.lenses.data.generator.http

import cats.effect.IO
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.Request
import org.http4s.circe._
import io.circe.Json
import org.http4s.Method
import org.http4s.dsl.io._
import org.http4s.Headers
import org.http4s.Header
import org.apache.avro
import org.apache.avro.Schema
import io.circe.generic.semiauto.deriveDecoder
import io.lenses.data.generator.cli.Creds
import org.http4s.dsl.impl.Auth
import fs2.concurrent.Topic
import io.circe.Decoder

object LensesClient {
  final case class AuthToken(value: String) extends AnyVal

  def apply(
      baseUrl: Uri,
      client: Client[IO],
      creds: Creds
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

      override def login() = {
        val request =
          Request[IO](method = Method.POST, uri = baseUrl / "api" / "login")
            .withEntity(
              Json.obj(
                "user" -> Json.fromString(creds.user),
                "password" -> Json.fromString(creds.password)
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
            "cleanup.policy" -> Json.fromString("delete")
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

      override def countTopics(implicit auth: AuthToken): IO[Int] = {

        case class TopicResponse(totalTopicCount: Int)
        implicit val topicRespDecoder: Decoder[TopicResponse] = deriveDecoder
        implicit val entityDec = jsonOf[IO, TopicResponse]

        val request =
          Request[IO](
            method = Method.GET,
            uri = (baseUrl / "api" / "v2" / "kafka" / "topics").withQueryParams(
              Map(
                "page" -> "1",
                "pageSize" -> "10"
              )
            )
          )
            .withHeaders(
              Header("X-Kafka-Lenses-Token", auth.value)
            )
        client.expect[TopicResponse](request).map {
          _.totalTopicCount
        }
      }

    }
}

import LensesClient.AuthToken
trait LensesClient {
  def login(): IO[AuthToken]
  def createTopic(topicName: String)(implicit auth: AuthToken): IO[Unit]
  def countTopics(implicit auth: AuthToken): IO[Int]
  def setTopicMetadata(topicName: String, schema: avro.Schema)(implicit
      auth: AuthToken
  ): IO[Unit]
}
