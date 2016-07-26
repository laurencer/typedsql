package com.rouesnel.typedsql.examples.coppersmith

import java.util.Date

import com.rouesnel.typedsql._
import com.rouesnel.typedsql.test._
import commbank.coppersmith.Feature.Value
import commbank.coppersmith.FeatureValue

class CoppersmithSqlSpec extends TypedSqlSpec { def is = s2"""
  Basic tests $basic
"""

  def basic = {
    val customers       = createDataSource(Customer(123, "Bob", "Brown", new Date().getTime.toInt, "my@email.com", Some("MALE"), 20))
    val orders          = createDataSource[Order]()
    val orderLineItems  = createDataSource[OrderLineItem]()
    val payments        = createDataSource[Payment]()


    val dataSource: DataSource[TypedSqlCoppersmithExample.Row] = TypedSqlCoppersmithExample(
      TypedSqlCoppersmithExample.Sources(customers, orders, orderLineItems, payments),
      TypedSqlCoppersmithExample.Parameters(18)
    )

    val features: List[FeatureValue[Value]] =
      executeDataSource(dataSource)
        .flatMap(TypedSqlCoppersmithExample.Features.generate)


    println()
    println(features)
    println()

    ok
  }
}
