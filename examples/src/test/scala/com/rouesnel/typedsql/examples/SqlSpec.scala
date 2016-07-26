package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._
import com.rouesnel.typedsql.test._

class SqlSpec extends TypedSqlSpec { def is = s2"""
  Basic tests $basic
"""

  def basic = {
    val people          = createDataSource(Person("Bob", "Brown", 28))
    val manualSqlStruct = createDataSource[ManualSqlStruct]()

    val sqlDataSource: DataSource[SqlQueryExample.Row] = SqlQueryExample(
      SqlQueryExample.Sources(people, manualSqlStruct, people),
      SqlQueryExample.Parameters(18)
    )

    val chainedDataSource = ChainedExample(
      ChainedExample.Sources(sqlDataSource),
      ChainedExample.Parameters()
    )

    val result        = executeDataSource(sqlDataSource)
    val resultChained = executeDataSource(chainedDataSource)

    println()
    println(result)
    println()

    result must not(beEmpty)

    val record = result.head
    record.intValue     must beEqualTo(1)
    record.doubleValue  must beEqualTo(1.3)
    record.mapValue     must beEqualTo(Map("key" -> 1.0, "key2" -> 2.0))

    record.structValue.col1 must beEqualTo(1.0)
    record.structValue.col2 must beEqualTo("stringvalue")
    record.structValue.col3 must beEqualTo(0)

    record.namedStructValue.fieldName  must beEqualTo(1)
    record.namedStructValue.field2     must beEqualTo(2)
    ok
  }
}
