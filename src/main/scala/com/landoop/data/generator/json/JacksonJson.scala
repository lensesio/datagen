package com.landoop.data.generator.json


import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.reflect.ClassTag

private[landoop] object JacksonJson {
  val Mapper: ObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m.setSerializationInclusion(Include.NON_NULL)
    m.setSerializationInclusion(Include.NON_EMPTY)
    m
  }

  def toJson[T](value: T): String = Mapper.writeValueAsString(value)


  def asJson[T](value: T): JsonNode = Mapper.valueToTree(value)


  def fromJson[T](json: String)(implicit ct: ClassTag[T]): T = Mapper.readValue(json, ct.runtimeClass).asInstanceOf[T]


  def toMap[V](json: String)(implicit m: Manifest[V]): Map[String, V] = fromJson[Map[String, V]](json)

}
