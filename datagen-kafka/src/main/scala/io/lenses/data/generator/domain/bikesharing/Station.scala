package io.lenses.data.generator.domain.bikesharing

import io.lenses.data.generator.JdbcReader

case class Station(id: Int,
                   station: String,
                   municipality: String,
                   lat: Double,
                   lng: Double)

object Station {
  def fromDb(): Iterable[Station] = {
    val dbPath = getClass.getResource("/hubway.db").toURI.getPath
    new JdbcReader(s"jdbc:sqlite:$dbPath")
      .read("SELECT * FROM stations"){rs=>
        Station(
          rs.getInt("id"),
          rs.getString("station"),
          rs.getString("municipality"),
          rs.getString("lat").toDouble,
          rs.getString("lng").toDouble
        )
      }
  }
}