package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.{DataSource, SqlQuery}

@SqlQuery object MissingSourcesExample {

  case class Parameters(value: Int)

  def query =
    """
      SELECT ${value} as int_value
    """
}