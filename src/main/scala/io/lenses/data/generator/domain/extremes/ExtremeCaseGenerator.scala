package io.lenses.data.generator.domain.extremes

import io.lenses.data.generator.domain.publishing.Publisher
import io.lenses.data.generator.domain.publishing.PublisherSchema
import io.lenses.data.generator.domain.publishing.GeneratorHelper
import io.lenses.data.generator.domain.Generator
import io.lenses.data.generator.config.DataGeneratorConfig
import java.util.UUID.randomUUID

object ExtremeCaseGenerator {
  import Publisher.stringKeyPublisher
  import Generator._
  import Random._
  implicit val keyStringRand = KeyGenerator[String](Random(randomUUID.toString))
  
  def sizedMessageGenerator(messageSize: Int, messagesAmount: Int): Generator = {
    def nextLargeString = String.valueOf(Array.fill(messageSize)(scala.util.Random.nextPrintableChar()))
    implicit val valueStringRand = Random[String](nextLargeString)

    val msgs: Stream[SimpleMessage] = Stream.continually(Random[SimpleMessage].next).take(messagesAmount)
    GeneratorHelper.stringKeyGenerator[SimpleMessage](_.key, msgs)
  }

  def nestedMessageGenerator(messagesAmount: Int): Generator = {
    implicit val valueStringRand = Random[String](randomUUID.toString)

    val msgs: Stream[NestedMessage] = Stream.continually(Random[NestedMessage].next).take(messagesAmount)
    GeneratorHelper.stringKeyGenerator[NestedMessage](_.key, msgs)
  }
}
