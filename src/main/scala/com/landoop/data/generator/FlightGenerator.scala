package com.landoop.data.generator

import java.time.{Instant, ZoneOffset}

import io.kotlintest.datagen.AirportProducer
import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.kafka.Producers
import org.apache.avro.{LogicalTypes, SchemaBuilder}
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object FlightGenerator {

  val config =
    DataGeneratorConfig("localhost:9092", "http://localhost:8081", 1, true)
  val props =
    Producers.getAvroValueProducerProps(classOf[StringSerializer])(config)
  implicit val producer: KafkaProducer[String, Any] =
    new KafkaProducer[String, Any](props)

  val startTime = Instant.now().atOffset(ZoneOffset.UTC).minusYears(1)

  val time = SchemaBuilder.builder().longType()
  LogicalTypes.timestampMillis().addToSchema(time)
  val schema = SchemaBuilder
    .record("flight")
    .fields()
    //.name("departure_time").`type`(time).noDefault()
    .requiredLong("departure_time")
    .requiredString("departure_airport")
    .requiredString("departure_airport_code")
    .requiredString("arrival_airport")
    .requiredString("arrival_airport_code")
    .endRecord()

  for (k <- 1 to 5000000) {
    val departure = AirportProducer.INSTANCE.produce()
    val arrival = AirportProducer.INSTANCE.produce()
    val record = new GenericData.Record(schema)
    record.put(
      "departure_time",
      startTime.plusMinutes(k).toInstant.toEpochMilli
    )
    record.put("departure_airport", departure.getName)
    record.put("departure_airport_code", departure.getCode)
    record.put("arrival_airport", arrival.getName)
    record.put("arrival_airport_code", arrival.getCode)
    val precord = new ProducerRecord[String, Any]("flights4", record)
    producer.send(precord)
  }

  producer.close()
  println("Completed")
}
