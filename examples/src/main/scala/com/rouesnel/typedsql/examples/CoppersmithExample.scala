package com.rouesnel.typedsql.examples
package coppersmith

import org.apache.hadoop.fs.Path
import com.twitter.scalding.{Config, Execution, TypedPipe}
import org.joda.time.DateTime
import commbank.coppersmith.api.{DataSource => _, _}
import scalding._
import Coppersmith._
import EavtText.{EavtEnc, eavtByDay}
import com.rouesnel.typedsql._
import com.rouesnel.typedsql.examples.coppersmith.TypedSqlCoppersmithExample.{Parameters, Sources}
import commbank.coppersmith.thrift.Eavt

import com.rouesnel.typedsql.DataSource

@SqlQuery
object TypedSqlCoppersmithExample {

  def query(minimumAge: Int)(customers: Unpartitioned[Customer],
                             orders: Unpartitioned[Order],
                             orderLineItems: Unpartitioned[OrderLineItem],
                             payments: Unpartitioned[Payment]) =
    """
      SELECT c.customer_id     as customer_id,
             c.age             as age,
             COUNT(o.order_id) as number_of_orders,
             SUM(oli.item_discount * oli.item_quantity) as amount_discounted,
             SUM(oli.item_price * oli.item_quantity)    as gross_amount_spent
       FROM ${customers} c
         LEFT OUTER JOIN ${orders} o ON c.customer_id = o.customer_id
         LEFT OUTER JOIN ${orderLineItems} oli ON o.order_id = oli.order_id
       WHERE c.age > ${minimumAge}
       GROUP BY c.customer_id, c.age
    """

  object Features extends FeatureSet[Row] {
    val namespace = "typedsql.example"

    def entity(row: Row) = row.customerId.toString

    val source = From[Row]() // FeatureSource (see above)
    val select = source.featureSetBuilder(namespace, entity)

    val customerAge = select(_.age.toInt)
      .asFeature(Continuous, "CUSTOMER_AGE", Some(MinMaxRange(0, 130)), "Age of customer in years")

    val orderCount = select(_.numberOfOrders)
      .asFeature(Continuous, "ORDER_COUNT", "Number of orders the customer has made.")

    val grossAmountSpent = select(_.grossAmountSpent).asFeature(
      Continuous,
      "GROSS_AMOUNT_SPENT",
      "Amount spent by the customer (before discounts).")

    val amountDiscounted = select(_.amountDiscounted).asFeature(
      Continuous,
      "MONEY_SAVED",
      "Amount of money saved by the customer due to discounts on ordered items.")

    val amountSpent = select(row => row.grossAmountSpent - row.amountDiscounted).asFeature(
      Continuous,
      "NET_AMOUNT_SPENT",
      "Amount spent by the customer (reduced by any discounts). Also known as revenue generated by customer.")

    val features = List(customerAge, orderCount, grossAmountSpent, amountDiscounted, amountSpent)
  }
}
