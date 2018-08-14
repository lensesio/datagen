package com.landoop.data.generator

import java.nio.charset.StandardCharsets
import java.util.Collections

import com.landoop.data.generator.stocks.{Exchange, Tick}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, Printer}
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import org.apache.pulsar.shade.org.apache.avro.SchemaBuilder

object TickSchema extends Schema[Tick] {

  val info = new SchemaInfo()
  info.setName("")
  info.setProperties(Collections.emptyMap())
  info.setType(SchemaType.JSON)
  info.setSchema(
    SchemaBuilder.record("tick").fields()
      .requiredString("symbol")
      .requiredString("name")
      .requiredString("category")
      .requiredDouble("bid")
      .requiredDouble("ask")
      .requiredBoolean("etf")
      .requiredInt("lotSize")
      .name("exchange").`type`(SchemaBuilder.record("exchange").fields().requiredString("name").requiredString("symbol").endRecord()).noDefault()
      .endRecord().toString().getBytes("UTF8"))

  implicit val encoderExchange: Encoder[Exchange] = deriveEncoder[Exchange]
  implicit val decoderExchange: Decoder[Exchange] = deriveDecoder[Exchange]
  implicit val encoder: Encoder[Tick] = deriveEncoder[Tick]
  implicit val decoder: Decoder[Tick] = deriveDecoder[Tick]

  override def encode(t: Tick): Array[Byte] = Printer.spaces2.pretty(encoder(t)).getBytes
  override def decode(bytes: Array[Byte]): Tick = io.circe.jawn.decode[Tick](new String(bytes, StandardCharsets.UTF_8)).right.get
  override def getSchemaInfo: SchemaInfo = info
}
