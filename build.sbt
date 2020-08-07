import Dependencies._

ThisBuild / scalaVersion := "2.13.3"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "io.lenses"
ThisBuild / organizationName := "datagen"

lazy val root = (project in file("."))
  .settings(
    name := "datagen",
    scalacOptions ++= Seq(
    ),
    libraryDependencies ++= Seq(
      `scalatest` % Test,
      Dependencies.`config`,
      `joda-time`,
      `slf4j-api`,
      `scala-logging`,
      `logback`,
      `univocity-parsers`,
      `kafka-client`,
      `kafka-avro-serializer`,
      `avro4s-core`,
      `jackson-databind`,
      `jackson-module-scala`,
      `jackson-dataformat-xml`,
      `sqlite-jdbc`,
      `pbdirect`,
      `kotlintest-datagen`,
      `case-app`,
      `scalacheck`,
      `enumeratum`,
      `http4s-blaze-client`,
      `http4s-circe`,
      `http4s-dsl`,
      `circe-generic`
    ),
    resolvers ++= Seq(
      "Confluent" at "https://packages.confluent.io/maven/",
      "Repo2" at "https://dl.bintray.com/beyondthelines/maven/",
      "Repo3" at "https://dl.bintray.com/julien-lafont/maven/"
    ),
    mainClass in (Compile, run) := Some("io.lenses.data.generator.Program")
  )
  .enablePlugins(ProtobufPlugin)

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
