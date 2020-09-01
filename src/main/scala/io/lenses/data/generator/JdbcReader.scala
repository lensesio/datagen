package io.lenses.data.generator

import java.sql.DriverManager
import java.sql.ResultSet

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class JdbcReader(connection: String) extends AutoCloseable {

  if (connection == null || connection.trim.isEmpty) {
    throw new IllegalArgumentException("Invalid connection string provided for the store.")
  }

  private val dbConnection = Try {
    DriverManager.getConnection(connection)
  } match {
    case Success(value) => value
    case Failure(exception) => throw new IllegalArgumentException(s"Invalid connection provided.[$connection] can not be opened.", exception)
  }

  override def close(): Unit = {
    dbConnection.close()
  }

  def read[T](sql: String)(fn: ResultSet => T): Iterable[T] = {
    val statement = dbConnection.createStatement()

    val resultSet = statement.executeQuery(sql)
    new Iterable[T] {
      override def iterator: Iterator[T] = new Iterator[T] {
        override def hasNext: Boolean = {
          val next = resultSet.next()
          if (!next) {
            Try(resultSet.close())
            Try(statement.close())
          }
          next
        }
        override def next(): T = fn(resultSet)
      }
    }
  }
}