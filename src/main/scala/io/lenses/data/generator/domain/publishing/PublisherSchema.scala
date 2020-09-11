package io.lenses.data.generator.domain.publishing

import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.Record
import com.sksamuel.avro4s.ToRecord
import io.lenses.data.generator.json.JacksonJson
import io.lenses.data.generator.json.JacksonXml
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import pbdirect._

sealed case class PublisherSchema[A, B, S <: Serializer[_]] private (toSerializable: A => B, serClass: Class[S])

object PublisherSchema {
  def integer[A](f: A => Int) =
    PublisherSchema[A, Int, IntegerSerializer](a => new Integer(f(a)), classOf[IntegerSerializer])

  def string[A](f: A => String): PublisherSchema[A, String, StringSerializer] =
    PublisherSchema(f(_), classOf[StringSerializer])

  def json[A]: PublisherSchema[A, String, StringSerializer] =
    PublisherSchema(JacksonJson.toJson _, classOf[StringSerializer])

  def xml[A]: PublisherSchema[A, String, StringSerializer] =
    PublisherSchema(JacksonXml.toXml _, classOf[StringSerializer])
  
  def avro[A: Encoder: SchemaFor]: PublisherSchema[A, Record, KafkaAvroSerializer] =
    PublisherSchema(ToRecord[A].to _, classOf[KafkaAvroSerializer])

  def proto[A <: AnyRef](implicit writer: PBWriter[A]): PublisherSchema[A, Array[Byte], ByteArraySerializer] =
    PublisherSchema((a: A) => a.toPB, classOf[ByteArraySerializer])

}