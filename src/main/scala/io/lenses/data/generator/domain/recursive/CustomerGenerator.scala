package io.lenses.data.generator.domain.recursive

import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.domain.Generator
import io.lenses.data.generator.kafka.Producers
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.DateTime

object CustomerGenerator extends Generator {
  private val schema = SchemaBuilder
    .record("customer")
    .namespace("io.lenses")
    .fields
    .name("name")
    .`type`(SchemaBuilder.builder().stringType())
    .noDefault()
    .name("policyId")
    .`type`(SchemaBuilder.builder().stringType())
    .noDefault()
    .name("link")
    .`type`
    .unionOf
    .nullType
    .and
    .`type`("customer")
    .endUnion
    .nullDefault
    .endRecord
  override def avro(
    topic: String
  )(implicit config: DataGeneratorConfig): Unit = {
    val props = Producers.getAvroValueProducerProps(classOf[StringSerializer])
    implicit val producer: KafkaProducer[String, GenericRecord] =
      new KafkaProducer[String, GenericRecord](props)

    generate().foreach { customer =>
      val record = new ProducerRecord(topic, DateTime.now.toString(), customer)
      producer.send(record)
    }
    producer.flush()
    producer.close()
  }
  protected def generate(): List[GenericRecord] = {
    List(
      generateCustomer(
        "jack smith",
        "p123",
        Some(generateCustomer("anne smith", "p123", None))
      ),
      generateCustomer("maria sharapova", "p3", None)
    )
  }
  private def generateCustomer(name: String,
                               policyId: String,
                               link: Option[GenericRecord]) = {
    val record = new GenericData.Record(schema)
    record.put("name", name)
    record.put("policyId", policyId)
    link.foreach(record.put("link", _))
    record
  }
  override def json(topic: String)(implicit config: DataGeneratorConfig): Unit =
    ???
  override def xml(topic: String)(implicit config: DataGeneratorConfig): Unit =
    ???
  override def protobuf(topic: String)(
    implicit config: DataGeneratorConfig
  ): Unit = ???
}
