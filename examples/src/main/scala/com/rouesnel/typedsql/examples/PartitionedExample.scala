package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

object PartitionSchemes {
  type ByYearMonth = { def year: String; def month: String }
  type ByYear = { def year: String }
}

@SqlQuery
object PartitionedExampleStep1 {

  type Partitions = PartitionSchemes.ByYearMonth

  def query(id: String, year: String, month: String, age: Int)
           (people: Unpartitioned[com.rouesnel.typedsql.Person]) =
    """
      SELECT ${id} as id, p.*, ${year} as year, ${month} as month
      FROM ${people} p
      WHERE p.age > ${age}
    """
}
@SqlQuery
object PartitionedExampleStep2 {

  type Partitions = PartitionSchemes.ByYear

  def query(people1: Partitioned[PartitionedExampleStep1.Row, { def year: String; def month: String }],
            people2: Partitioned[PartitionedExampleStep1.Row, PartitionSchemes.ByYearMonth]) =
    """
      SELECT p1.firstname, p1.year, p1.month, p2.id, p1.lastname, p1.age
      FROM ${people1} p1
        INNER JOIN ${people2} p2 ON p1.firstname = p2.firstname
    """
}

@SqlQuery
object PartitionedExampleStep3 {
  def query(step2: PartitionedExampleStep2.DataSource) =
    """
      SELECT mapped.year, mapped.month, mapped.firstname, mapped.lastname, mapped.id, mapped.age
      FROM ${step2} mapped
    """
}