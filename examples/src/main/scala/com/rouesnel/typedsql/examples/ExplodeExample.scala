package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.{DataSource, SqlQuery}

@SqlQuery
object ExplodeExample {

  def query() =
    """
      SELECT EXPLODE(MAP("v", 1, "k", 2))
    """
}
