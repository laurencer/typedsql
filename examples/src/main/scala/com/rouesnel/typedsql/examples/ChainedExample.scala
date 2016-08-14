package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.{DataSource, SqlQuery}


@SqlQuery object ChainedExample {

  def query(upstream: DataSource[SqlQueryExample.Row]) =
    """
      SELECT int_value, COUNT(*) as count
      FROM ${upstream}
      GROUP BY int_value
    """
}