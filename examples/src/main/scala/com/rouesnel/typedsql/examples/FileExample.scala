package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery
object FileExample {

  def query(people: Unpartitioned[Person], firstName: String) =
    FromFile("examples/src/main/hive/example.sql")

}
