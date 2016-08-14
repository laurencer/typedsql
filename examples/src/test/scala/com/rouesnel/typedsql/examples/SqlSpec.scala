package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._
import com.rouesnel.typedsql.test._

class SqlSpec extends TypedSqlSpec { def is = s2"""
  Basic tests $basic
"""

  def basic = {
    val people          = createDataSource(Person("Bob", "Brown", 28))
    val manualSqlStruct = createDataSource[ManualSqlStruct]()

    val sqlDataSource: DataSource[SqlQueryExample.Row] =
      SqlQueryExample.query(18)(people, manualSqlStruct, people)

    val chainedDataSource = ChainedExample.query(sqlDataSource)

    val composed = (SqlQueryExample.query(18) _).tupled andThen (ChainedExample.query _)
    val composedDataSource = composed(people, manualSqlStruct, people)

    val result         = executeDataSource(sqlDataSource)
    val resultChained  = executeDataSource(chainedDataSource)
    val resultComposed = executeDataSource(composedDataSource)

    println()
    println(result)
    println()

    result must not(beEmpty)

    resultChained must beEqualTo(resultComposed)

    val record = result.head
    record.intValue     must beEqualTo(1)
    record.doubleValue  must beEqualTo(1.3)
    record.mapValue     must beEqualTo(Map("key" -> 1.0, "key2" -> 2.0))

    record.structValue.col1 must beEqualTo(1.0)
    record.structValue.col2 must beEqualTo("stringvalue")
    record.structValue.col3 must beEqualTo(0)

    record.namedStructValue.fieldName  must beEqualTo(1)
    record.namedStructValue.field2     must beEqualTo(2)

    record.udfApplied must beEqualTo("0,1,2,3,4,5")
    ok
  }
}
