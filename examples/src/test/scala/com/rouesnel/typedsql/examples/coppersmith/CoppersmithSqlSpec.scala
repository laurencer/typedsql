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
    def date = new Date().getTime.toInt

    val customers       = createDataSource(
      Customer(123, "Bob",   "Brown", date, "bb@email.com", Some("MALE"), 32)
    , Customer(124, "Diana", "Brown", date, "db@email.com", Some("FEMALE"), 31)
    , Customer(125, "Aaron", "Brown", date, "ab@email.com", Some("FEMALE"), 14)
    )
    val orders          = createDataSource(
      Order("O_000098", 123, date, None, None, None)
    )
    val orderLineItems  = createDataSource[OrderLineItem]()
    val payments        = createDataSource[Payment]()


    val dataSource: TypedSqlCoppersmithExample.DataSource =
      TypedSqlCoppersmithExample.query(18)(customers, orders, orderLineItems, payments)

    val features: List[FeatureValue[Value]] =
      executeDataSource(dataSource)
        .flatMap(TypedSqlCoppersmithExample.Features.generate)

    println()
    println(features)
    println()

    ok
  }
}
