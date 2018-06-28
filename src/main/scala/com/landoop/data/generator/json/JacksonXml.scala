package com.landoop.data.generator.json

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.reflect.ClassTag

private[landoop] object JacksonXml {
  val Mapper: XmlMapper with ScalaObjectMapper = {
    val m = new XmlMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m.setSerializationInclusion(Include.NON_NULL)
    m.setSerializationInclusion(Include.NON_EMPTY)
    //m.setDefaultUseWrapper(true)
    m
  }

  def toXml[T](value: T): String = Mapper.writeValueAsString(value)


  def asXml[T](value: T): JsonNode = Mapper.valueToTree(value)


  def fromXml[T](json: String)(implicit ct: ClassTag[T]): T = Mapper.readValue(json, ct.runtimeClass).asInstanceOf[T]


}