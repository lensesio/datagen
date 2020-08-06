import sbt._

object V {
  val scalatest = "3.1.1"
  val scopt = "3.7.1"
  val config = "1.2.1"
  val jodaTime = "2.9.9"
  val sl4jApi = "1.7.7"
  val scalaLogging = "3.9.2"
  val logback = "1.2.3"
  val kafka = "2.2.0"
  val confluent = "3.3.0"
  val jackson = "2.9.10"
  val sqlite = "3.27.2.1"
  val avro4s = "3.0.6"
  val pbdirect = "0.2.1"
  val protobuf = "3.0.0"
  val kotlintestDatagen = "0.10.0"
}

object Dependencies {
  lazy val `scalatest` = "org.scalatest" %% "scalatest" % V.scalatest
  lazy val `scopt` = "com.github.scopt" %% "scopt" % V.scopt
  lazy val `config` = "com.typesafe" % "config" % V.config
  lazy val `joda-time` = "joda-time" % "joda-time" % V.jodaTime
  lazy val `slf4j-api` = "org.slf4j" % "slf4j-api" % V.sl4jApi
  lazy val `scala-logging` =
    "com.typesafe.scala-logging" %% "scala-logging" % V.scalaLogging
  lazy val `logback` = "ch.qos.logback" % "logback-classic" % V.logback
  lazy val `univocity-parsers` = "com.univocity" % "univocity-parsers" % "2.7.5"
  lazy val `kafka-client` = "org.apache.kafka" % "kafka-clients" % V.kafka
  lazy val `kafka-avro-serializer` =
    "io.confluent" % "kafka-avro-serializer" % V.confluent
  lazy val `pbdirect` = "beyondthelines" %% "pbdirect" % V.pbdirect
//    lazy val `` = "io.protoless:protoless-core_$scalaMajorVersion:0.0.7"
//    lazy val `` = "io.protoless:protoless-generic_$scalaMajorVersion:0.0.7"
  lazy val `protobuf-java` =
    "com.google.protobuf" % "protobuf-java" % V.protobuf
  lazy val `avro4s-core` = "com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s

  lazy val `jackson-databind` =
    "com.fasterxml.jackson.core" % "jackson-databind" % V.jackson
  lazy val `jackson-module-scala` =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % V.jackson
  lazy val `jackson-dataformat-xml` =
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % V.jackson
  lazy val `sqlite-jdbc` = "org.xerial" % "sqlite-jdbc" % V.sqlite
  lazy val `kotlintest-datagen` =
    "io.kotlintest" % "kotlintest-datagen" % V.kotlintestDatagen
}
