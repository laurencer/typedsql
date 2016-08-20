package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.test._

class ParameterSpec extends TypedSqlSpec { def is = s2"""
  Basic tests $basic
"""

  def basic = {

    val testBool: Boolean  = false
    val testByte: Byte = 1
    val testShort: Short = 3
    val testInt: Int = 5
    val testLong: Long = 234
    val testFloat: Float = 3.2f
    val testDouble: Double = 34.5
    val testDate: java.sql.Date = new java.sql.Date(115, 1, 21)
    val testString: String = "My test string\"which needs interpolation."

    val result = executeDataSource(ParameterExample.query(
      testBool, testByte, testShort, testInt, testLong, testFloat, testDouble, testDate, testString
    ))

    println()
    println(result)
    println()

    result must not(beEmpty)
    val row :: Nil = result.toList

    row.boolValue must beEqualTo(testBool)
    row.tinyIntValue must beEqualTo(testByte)
    row.smallIntValue must beEqualTo(testShort)
    row.intValue must beEqualTo(testInt)
    row.longValue must beEqualTo(testLong)
    row.floatValue.toFloat must beEqualTo(testFloat)
    row.doubleValue must beEqualTo(testDouble)
    row.dateValue must beEqualTo(testDate.toString)
    row.stringValue must beEqualTo(testString)

    ok
  }
}
