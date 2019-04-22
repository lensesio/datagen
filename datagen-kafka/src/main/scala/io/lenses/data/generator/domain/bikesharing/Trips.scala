package io.lenses.data.generator.domain.bikesharing

import org.joda.time.DateTime

case class Trips(
                  id: Int,
                  duration: Int,
                  start_date: DateTime,
                  start_station: Int,
                  end_date: DateTime,
                  end_station: Int,
                  bike_number: String,
                  sub_type: String,
                  zip_code: String,
                  birth_date: Int,
                  gender: String
                )