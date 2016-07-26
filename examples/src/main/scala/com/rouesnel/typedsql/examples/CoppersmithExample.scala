package com.rouesnel.typedsql.examples
package coppersmith

import org.apache.hadoop.fs.Path
import com.twitter.scalding.{Config, Execution, TypedPipe}
import org.joda.time.DateTime
import commbank.coppersmith.api.{DataSource => _, _}
import scalding._
import Coppersmith._
import EavtText.{EavtEnc, eavtByDay}
import com.rouesnel.typedsql.SqlQuery
import com.rouesnel.typedsql.examples.coppersmith.TypedSqlCoppersmithExample.{Parameters, Sources}
import commbank.coppersmith.thrift.Eavt

import com.rouesnel.typedsql.DataSource


@SqlQuery object TypedSqlCoppersmithExample {

  def query =
    """
      SELECT c.customer_id as customer_id,
             19         as age,
             COUNT(o.order_id) as number_of_orders
       FROM ${customers} c
         RIGHT OUTER JOIN ${orders} o ON c.customer_id = o.customer_id
       WHERE c.age > ${minimumAge}
       GROUP BY c.customer_id
    """

  case class Sources(customers: DataSource[Customer],
                     orders: DataSource[Order],
                     orderLineItems: DataSource[OrderLineItem],
                     payments: DataSource[Payment])

  case class Parameters(minimumAge: Int)


  object Features extends BasicFeatureSet[Row] {
    val namespace          = "typedsql.example"
    def entity(row: Row) = row.customerId.toString

    val customerAge = basicFeature[Integral](
      "CUSTOMER_AGE", "Age of customer in years", Continuous, Some(MinMaxRange[Integral](0, 130))
    )(row => row.age)

    val orderCount = basicFeature[Integral](
      "ORDER_COUNT", "Number of orders the customer has made.", Continuous, Some(MinMaxRange[Integral](0, 130))
    )(row => row.numberOfOrders)

    val features = List(customerAge, orderCount)
  }
}
