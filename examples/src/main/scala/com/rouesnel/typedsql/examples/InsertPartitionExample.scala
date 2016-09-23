package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.examples.coppersmith._

import com.rouesnel.typedsql._

@SqlQuery
object InsertPartitionExample {

  type Partitions = PartitionSchemes.ByYearMonth

  def query(year: Int, month: Int)(
      orders: Partitioned[PartitionedOrders.Row, PartitionSchemes.ByYearMonth]) =
    """
      SELECT o.year,
             o.month,
             o.customer_id,
             COUNT(*) as number_of_orders
      FROM ${orders} o
      WHERE o.year = ${year} AND
            o.month = ${month} AND
            o.cancelled IS NULL
      GROUP BY o.year, o.month, o.customer_id
    """
}

@SqlQuery
object PartitionedOrders {
  type Partitions = PartitionSchemes.ByYearMonth

  def query(orders: Unpartitioned[Order]) =
    """
    SELECT o.*,
           from_unixtime(o.order_time, 'YYYY') as year,
           from_unixtime(o.order_time, 'MM') as month
    FROM ${orders} o
  """
}

@SqlQuery
object SelectFromPartitionedOrders {
  def query(orders: InsertPartitionExample.DataSource) =
    """
    SELECT * FROM ${orders}
  """
}
