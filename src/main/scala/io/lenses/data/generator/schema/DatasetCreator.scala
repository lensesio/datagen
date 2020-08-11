package io.lenses.data.generator.schema

import cats.effect.IO
import io.lenses.data.generator.http.LensesClient
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.Uri
import scala.concurrent.ExecutionContext
import cats.effect.ContextShift
import org.http4s.client.Client
import org.http4s.Request
import org.http4s.Method
import org.http4s.circe._
import io.circe.Json
import org.http4s.Status
import io.lenses.data.generator.cli.Creds
import doobie.util.transactor.Transactor
import io.lenses.data.generator.schema.pg.PostgresConfig
import io.lenses.data.generator.schema.converters.PostgresConverter
import cats.implicits._

trait DatasetCreator {
  def create(name: String, schema: Schema)(implicit
      ec: ExecutionContext,
      cs: ContextShift[IO]
  ): IO[Unit]
}

object DatasetCreator {
  def Kafka(
      lensesClient: LensesClient,
      lensesCreds: Creds
  ): DatasetCreator =
    new DatasetCreator {
      override def create(schemaName: String, schema: Schema)(implicit
          ec: ExecutionContext,
          cs: ContextShift[IO]
      ): IO[Unit] = {
        lensesClient.login(lensesCreds.user, lensesCreds.password).flatMap {
          implicit auth =>
            lensesClient
              .createTopic(schemaName) *> lensesClient.setTopicMetadata(
              schemaName,
              AvroConverter(schema, Some(schemaName))
            )
        }
      }
    }

  //TODO: add creds
  def Elasticsearch(httpClient: Client[IO], baseUrl: Uri): DatasetCreator =
    new DatasetCreator {

      override def create(name: String, schema: Schema)(implicit
          ec: ExecutionContext,
          cs: ContextShift[IO]
      ): IO[Unit] = {
        val body = Json.obj("mappings" -> ElasticsearchCoverter(schema, None))
        val request =
          Request[IO](method = Method.PUT, uri = baseUrl / name.toLowerCase())
            .withEntity(body)

        httpClient.run(request).use {
          case Status.Successful(r) => IO.unit
          case resp =>
            resp
              .as[String]
              .flatMap(b =>
                IO.raiseError(
                  new Exception(
                    s"Request failed with status ${resp.status.code} and body $b"
                  )
                )
              )
        }
      }

    }

  def Postgres(tx: Transactor[IO], config: PostgresConfig): DatasetCreator =
    new DatasetCreator {
      import doobie._
      import doobie.implicits._

      override def create(name: String, schema: Schema)(implicit
          ec: ExecutionContext,
          cs: ContextShift[IO]
      ): IO[Unit] = {
        implicit val logHandler = LogHandler.jdkLogHandler
        val converter = new PostgresConverter(config)
        val dll = converter(schema, Some(name))

        val createPgSchema =
          config.containingSchema.fold(0.pure[ConnectionIO])(schema =>
            Update0.apply(s"CREATE SCHEMA IF NOT EXISTS $schema ", None).run
          )

        val createTable = converter(schema, Some(name)).asDLL.update.run

        (createPgSchema *> createTable).transact(tx).void
      }
    }

}
