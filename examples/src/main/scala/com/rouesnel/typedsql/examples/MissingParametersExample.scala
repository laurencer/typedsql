package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.{DataSource, SqlQuery}

@SqlQuery object MissingParametersExample {
  case class Sources(upstream: DataSource[SqlQueryExample.Row])

  def query =
    """
      SELECT int_value, COUNT(*) as count
      FROM ${upstream}
      GROUP BY int_value
    """
}