package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.{DataSource, SqlQuery}

@SqlQuery
object MissingSourcesExample {

  def query(value: Int) =
    """
      SELECT ${value} as int_value
    """
}
