package io.lenses.data.generator.kafka

import java.util.Properties

import io.lenses.data.generator.config.DataGeneratorConfig
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import io.lenses.data.generator.config.DataGeneratorConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

object Producers {
  def getAvroValueProducerProps[T <: Serializer[_]](ser: Class[T])(implicit config: DataGeneratorConfig): Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ser)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry)
    props
  }

  def getStringValueProducerProps[T <: Serializer[_]](ser: Class[T])(implicit config: DataGeneratorConfig): Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ser)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry)
    props
  }

  def getProducerProps[K <: Serializer[_], V <: Serializer[_]](keySer: Class[K], valueSer:Class[V])(implicit config: DataGeneratorConfig): Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry)
    props
  }
}
