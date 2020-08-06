package io.lenses.data.generator.config

import com.typesafe.config.Config
import scala.jdk.CollectionConverters._

object ConfigExtension {

  implicit class Extractor(val config: Config) extends AnyVal {
    def readInt(key: String, default: Int): Int =
      get(key, default)(config.getInt)

    def readInt(key: String): Int = get(key)(config.getInt)

    def readLong(key: String, default: Long): Long =
      get(key, default)(config.getLong)

    def readLong(key: String): Long = get(key)(config.getLong)

    def readString(key: String, default: String): String =
      get(key, default)(config.getString)

    def readString(key: String): String = get(key)(config.getString)

    def readStringList(key: String): List[String] =
      get(key)(config.getStringList).asScala.toList

    def readStringList(key: String, default: List[String]): List[String] =
      get(key, default)(k => config.getStringList(k).asScala.toList)

    def readBoolean(key: String, default: Boolean): Boolean =
      get(key, default)(config.getBoolean)

    def readBoolean(key: String): Boolean = get(key)(config.getBoolean)

    def readDouble(key: String, default: Double): Double =
      get(key, default)(config.getDouble)

    def readDouble(key: String): Double = get(key)(config.getDouble)

    private def get[T](key: String, default: T)(extractor: (String) => T) = {
      if (config.hasPath(key)) extractor(key)
      else default
    }

    private def get[T](key: String)(extractor: (String) => T) = {
      if (config.hasPath(key)) extractor(key)
      else
        throw new IllegalArgumentException(
          s"Missing configuration entry for '$key'"
        )
    }
  }

}
