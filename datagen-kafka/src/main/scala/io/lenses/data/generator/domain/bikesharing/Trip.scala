package io.lenses.data.generator.domain.bikesharing

import java.time.LocalDateTime
import java.time.chrono.IsoChronology
import java.time.format.DateTimeFormatter
import java.util.Locale

import io.lenses.data.generator.JdbcReader

case class Trip(id: Int,
                duration: Long,
                start_date: LocalDateTime,
                start_station: Int,
                end_date: LocalDateTime,
                end_station: Int,
                bike_number: String,
                sub_type: String,
                zip_code: Option[String],
                birth_date: Option[Int],
                gender: Option[String])

object Trip {
  def fromDb(): Iterable[Trip] = {
    val dbPath = getClass.getResource("/hubway.db").toURI.getPath

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      .withLocale(Locale.ROOT)
      .withChronology(IsoChronology.INSTANCE)

    new JdbcReader(s"jdbc:sqlite:$dbPath")
      .read("SELECT * FROM trips") { rs =>
        var zipCode = rs.getString("zip_code")
        if (zipCode.startsWith("'")) {
          zipCode = zipCode.drop(1)
        }
        Trip(
          rs.getInt("id"),
          rs.getInt("duration"),
          LocalDateTime.parse(rs.getString("start_date"), formatter),
          rs.getInt("start_station"),
          LocalDateTime.parse(rs.getString("end_date"), formatter),
          rs.getInt("end_station"),
          rs.getString("bike_number"),
          rs.getString("sub_type"),
          ifEmptyNone(zipCode),
          ifZeroNone(rs.getDouble("birth_date").toInt),
          ifEmptyNone(rs.getString("gender"))
        )
      }
  }
  private def ifEmptyNone(s: String): Option[String] = if (s.isEmpty) None else Some(s)
  private def ifZeroNone(i: Int): Option[Int] = if (i <= 0) None else Some(i)
}
