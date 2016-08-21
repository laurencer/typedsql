package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.{DataSource, SqlQuery}

@SqlQuery
object ChainedExample {

  def query(upstream: SqlQueryExample.DataSource) =
    """
      SELECT int_value, COUNT(*) as count
      FROM ${upstream}
      GROUP BY int_value
    """
}
