package io.lenses.data.generator

import org.scalatest.flatspec.AnyFlatSpec
import io.lenses.data.generator.cli.Creds
import scala.concurrent.duration.FiniteDuration
import cats.effect.IO
import cats.effect.Timer
import cats.effect.ContextShift
import java.util.concurrent.TimeoutException
import org.http4s.client.Client
import org.http4s.client.JavaNetClientBuilder
import cats.effect.Blocker
import io.lenses.data.generator.http.LensesClient
import org.http4s.Uri
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

class SmokeTest extends AnyFlatSpec with Matchers {
  val ec = scala.concurrent.ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)
  val blocker = Blocker.liftExecutionContext(ec)
  val httpClient: Client[IO] = JavaNetClientBuilder[IO](blocker).create

  val lensesBaseUrl = "http://localhost:3030"
  val lensesCreds = Creds("admin", "admin")
  val lensesClient =
    LensesClient(
      Uri.unsafeFromString(lensesBaseUrl),
      httpClient,
      lensesCreds,
      None
    )

  val elasticBaseUrl = "http://localhost:9200"
  val elasticCreds = Creds("elastic", "changeme")

  val postgresDb = "postgres"
  val postgresSchema = "blah"
  val postgresCreds = Creds("postgres", "mysecretpassword")

  val numKafkaDatasets = 10
  val numElasticDatasets = 11
  val numPostgresDatasets = 12

  val args = List(
    "gen-schemas",
    "--num-kafka-datasets",
    s"$numKafkaDatasets",
    "--num-elastic-datasets",
    s"$numElasticDatasets",
    "--num-postgres-datasets",
    s"$numPostgresDatasets",
    "--lenses-base-url",
    lensesBaseUrl,
    "--lenses-creds",
    s"${lensesCreds.user}:${lensesCreds.password}",
    "--elastic-base-url",
    s"$elasticBaseUrl",
    "--elastic-creds",
    s"${elasticCreds.user}:${elasticCreds.password}",
    "--postgres-database",
    s"$postgresDb",
    "--postgres-schema",
    s"$postgresSchema",
    "--postgres-creds",
    s"${postgresCreds.user}:${postgresCreds.password}"
  )

  implicit val authToken = Utils
    .retryUntilSuccessful(
      lensesClient.login(),
      1.second,
      15.seconds,
      "Waiting for Lenses to startup"
    )()
    .unsafeRunSync()

  //run gen-schema command with all the available options
  Main.main(args.toArray)

  "the generated Kafka topics" should s"be >= $numKafkaDatasets" in {
    lensesClient.countTopics.unsafeRunSync should be >= numKafkaDatasets
  }

  "the generated Elasticsearch indexes" should s"be >= $numElasticDatasets" in {
    new Elasticsearch(
      httpClient,
      Some(elasticCreds),
      Uri.unsafeFromString(elasticBaseUrl)
    ).countIndexes.unsafeRunSync() should be >= numElasticDatasets
  }

  "the generated Postgres table" should s"be >= $numPostgresDatasets" in {
    val xa = Transactor.fromDriverManager[IO](
      "org.postgresql.Driver", // driver classname
      s"jdbc:postgresql:${postgresDb}", // connect URL (driver-specific)
      postgresCreds.user,
      postgresCreds.password,
      Blocker.liftExecutionContext(ec)
    )

    implicit val logger = LogHandler.jdkLogHandler

    Query0[Int](
      s"SELECT count(*) FROM information_schema.tables WHERE table_schema = '$postgresSchema'"
    ).unique.transact(xa).unsafeRunSync() should be >= numPostgresDatasets
  }
}
