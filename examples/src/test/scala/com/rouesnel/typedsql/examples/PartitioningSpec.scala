package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._
import com.rouesnel.typedsql.DataSource.Strategy._
import com.rouesnel.typedsql.test._

class PartitioningSpec extends TypedSqlSpec { def is = s2"""
  Basic tests $basic
"""

  def basic = {

    val people = createDataSource(
      Person("Bob", "Brown", 28),
      Person("Sally", "Brown", 29),
      Person("Bob", "Dega", 10)
    )

    val step1a = PartitionedExampleStep1.query("1a", "2000", "11", 7)(people)

    val step1b =
      PartitionedExampleStep1.query("1b", "2001", "12", 18)(people)
        .persist(alwaysRefresh("test", "examples.test", "examples/test"))

    val step2 = PartitionedExampleStep2.query(step1a, step1b)

    val step3 = PartitionedExampleStep3.query(step2)

    println()
    println(executeDataSource(step1a))
    println(executeDataSource(step1b))
    println(executeDataSource(step2))
    println(executeDataSource(step3))
    println()

    ok
  }
}
