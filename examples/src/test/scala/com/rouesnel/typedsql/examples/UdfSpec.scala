package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._
import com.rouesnel.typedsql.test._

class UdfSpec extends TypedSqlSpec { def is = s2"""
  Basic tests $basic
"""

  def basic = {

    val result = executeDataSource(UdfExample.query)

    println()
    println(result)
    println()

    result must not(beEmpty)
    val row :: Nil = result.toList
    row.testBoolean must beEqualTo(UdfExample.testBoolean())
    row.testByte must beEqualTo(UdfExample.testByte())
    row.testShort must beEqualTo(UdfExample.testShort())
    row.testInt must beEqualTo(UdfExample.testInt())
    row.testLong must beEqualTo(UdfExample.testLong())
    row.testFloat must beEqualTo(UdfExample.testFloat())
    row.testDouble must beEqualTo(UdfExample.testDouble())
    row.testDate must beEqualTo(UdfExample.testDate().toString)
    row.testString must beEqualTo(UdfExample.testString())
    row.argTest must beEqualTo("false 0 24 32 64 96.1 128.2 2016-05-04 test")
    ok
  }
}
