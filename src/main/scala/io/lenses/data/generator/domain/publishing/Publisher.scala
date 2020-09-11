package io.lenses.data.generator.domain.publishing

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.serialization.Serializer
import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.domain.Generator
import io.lenses.data.generator.kafka.Producers
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.reflect.ClassTag

import com.sksamuel.avro4s.Record
import io.confluent.kafka.serializers.KafkaAvroSerializer

object Publisher {
  def stringKeyPublisher[A: ClassTag, V, VS <: Serializer[_]](f: A => String, valSchema: PublisherSchema[A, V, VS])(implicit config: DataGeneratorConfig) =
    new Publisher(PublisherSchema.string[A](f), valSchema)
}

class Publisher[A: ClassTag, K, KS <: Serializer[_], V, VS <: Serializer[_]](
    keySchema: PublisherSchema[A, K, KS],
    valSchema: PublisherSchema[A, V, VS]
)(implicit config: DataGeneratorConfig)
    extends StrictLogging {

  def publish(as: Stream[A], topic: String): Unit = {
    val name = implicitly[ClassTag[A]].runtimeClass.getSimpleName()
    val props = Producers.getProducerProps(keySchema.serClass, valSchema.serClass)
    val producer = new KafkaProducer[K, V](props)

    logger.info(s"Publishing $name data to '$topic'")
    try {
      as.foreach { a =>
        val record = new ProducerRecord[K, V](
          topic,
          keySchema.toSerializable(a),
          valSchema.toSerializable(a)
        )
        producer.send(record)
      }
      logger.info(s"Finished generating $name data to '$topic'")
    } catch {
      case t: Throwable =>
        logger.error(s"Failed to publish $name data to '$topic'", t)
    } finally {
      producer.close()
    }
  }
}
