package io.lenses.data.generator.domain.publishing

import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.SchemaFor
import io.lenses.data.generator.config.DataGeneratorConfig
import io.lenses.data.generator.domain.Generator
import scala.reflect.ClassTag
import pbdirect._

object GeneratorHelper {
  import Publisher.stringKeyPublisher
  
  def stringKeyGenerator[A <: AnyRef: Encoder: SchemaFor: PBWriter: ClassTag](getKey: A => String, msgs: Stream[A]) =
    new Generator {
      override def avro(topic: String)(implicit config: DataGeneratorConfig) =
        stringKeyPublisher(getKey, PublisherSchema.avro[A]).publish(msgs, topic)

      override def json(topic: String)(implicit config: DataGeneratorConfig): Unit =
        stringKeyPublisher(getKey, PublisherSchema.json[A]).publish(msgs, topic)

      override def xml(topic: String)(implicit config: DataGeneratorConfig): Unit =
        stringKeyPublisher(getKey, PublisherSchema.xml[A]).publish(msgs, topic)

      override def protobuf(topic: String)(implicit config: DataGeneratorConfig): Unit =
        stringKeyPublisher(getKey, PublisherSchema.proto[A]).publish(msgs, topic)
    }
}
