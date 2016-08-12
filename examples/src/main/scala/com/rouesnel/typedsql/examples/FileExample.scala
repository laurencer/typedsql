package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery object FileExample {

  case class Sources(people: DataSource[Person])

  case class Parameters(firstName: String)

  def query = FromFile("examples/src/main/hive/example.sql")

}