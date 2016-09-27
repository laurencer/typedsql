package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.DataSource.Strategy._
import com.rouesnel.typedsql._
import com.rouesnel.typedsql.examples.coppersmith.Order
import com.rouesnel.typedsql.test._

class InsertPartitionExampleSpec extends TypedSqlSpec { def is = s2"""
  TypedSQL jobs can append partitions to an existing table. $basic
"""

  def hiveTimeFor(year: Int, month: Int, day: Int): Long =
    new java.util.Date(year - 1900, month - 1, day).getTime / 1000

  def basic = {
    val orders = createDataSource(
      Order("O001", 1, hiveTimeFor(2016, 3, 11), Some(hiveTimeFor(2016, 3, 13)), None, None),
      Order("O002", 1, hiveTimeFor(2016, 3, 12), None, None, None),
      Order("O003", 1, hiveTimeFor(2016, 3, 13), None, Some(hiveTimeFor(2016, 3, 19)), None),
      Order("O004", 2, hiveTimeFor(2016, 3, 14), None, None, None),
      Order("O005", 1, hiveTimeFor(2016, 4, 11), None, Some(hiveTimeFor(2016, 3, 11)), None),
      Order("O006", 1, hiveTimeFor(2016, 4, 11), Some(hiveTimeFor(2016, 4, 15)), None, None),
      Order("O007", 1, hiveTimeFor(2016, 4, 12), None, None, None),
      Order("O008", 1, hiveTimeFor(2016, 4, 13), None, Some(hiveTimeFor(2016, 4, 19)), None),
      Order("O009", 2, hiveTimeFor(2016, 4, 14), None, None, None)
    )

    val partitionedOrders =
      PartitionedOrders
        .query(orders)
        .persist(reuseExisting("examples", "examples.orders", s"${testDir.resolve("tmp")}/examples/orders"))

    val firstInsert =
      InsertPartitionExample
        .query(2016, 3)(partitionedOrders)
        .persist(appendToExisting("examples", "examples.insert_partitions", s"${testDir.resolve("tmp")}/examples/insert_partition"))

    val secondInsert =
      InsertPartitionExample
        .query(2016, 4)(partitionedOrders)
        .persist(appendToExisting("examples", "examples.insert_partitions", s"${testDir.resolve("tmp")}/examples/insert_partition"))

    val selectFromTable = SelectFromPartitionedOrders.query(
      MaterialisedHiveTable[InsertPartitionExample.Row, PartitionSchemes.ByYearMonth](
        s"${testDir.resolve("tmp")}/examples/insert_partition", "examples.insert_partitions"
      )
    )

    val firstInsertResults = executeDataSource(firstInsert)
    val firstSelectResults = executeDataSource(selectFromTable)
    val secondInsertResults = executeDataSource(secondInsert)
    val secondSelectResults = executeDataSource(selectFromTable)

    println()
    println("First insert:")
    firstInsertResults.foreach(r => println("\t" + r.toString))
    println("First select:")
    firstSelectResults.foreach(r => println("\t" + r.toString))
    println("Second insert:")
    secondInsertResults.foreach(r => println("\t" + r.toString))
    println("Second select:")
    secondSelectResults.foreach(r => println("\t" + r.toString))
    println()

    firstSelectResults must containTheSameElementsAs(List(
      SelectFromPartitionedOrders.Row(1, 2, "2016", "03"),
      SelectFromPartitionedOrders.Row(2, 1, "2016", "03")
    ))

    secondSelectResults must containTheSameElementsAs(List(
      SelectFromPartitionedOrders.Row(1, 2, "2016", "03"),
      SelectFromPartitionedOrders.Row(2, 1, "2016", "03"),
      SelectFromPartitionedOrders.Row(1, 2, "2016", "04"),
      SelectFromPartitionedOrders.Row(2, 1, "2016", "04")
    ))

    ok
  }
}
