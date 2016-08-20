package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery
object UdfExample {

  @UDF def myUdf(bool: Boolean, tinyInt: Byte, short: Short, int: Int, long: Long, float: Float,
                 double: Double, date: java.sql.Date, string: String): String =
    s"${bool} ${tinyInt} ${short} ${int} ${long} ${float} ${double} ${date} ${string}"

  @UDF def testBoolean(): Boolean = false
  @UDF def testByte(): Byte = 0
  @UDF def testShort(): Short = 0
  @UDF def testInt(): Int = 0
  @UDF def testLong(): Long = 0
  @UDF def testFloat(): Float = 0.0F
  @UDF def testDouble(): Double = 0.0
  @UDF def testDate(): java.sql.Date = new java.sql.Date(115, 3, 16)
  @UDF def testString(): String = ""

  def query =
    """
      SELECT myUdf(false, cast(0 as tinyint), cast(24 as smallint), 32, cast(64 as bigint), cast(96.1 as float), cast(128.2 as double), cast("2016-05-04" as date), "test") as arg_test,
            testBoolean() as test_boolean,
            testByte() as test_byte,
            testShort() as test_short,
            testInt() as test_int,
            testLong() as test_long,
            testFloat() as test_float,
            testDouble() as test_double,
            cast(testDate() as string) as test_date,
            testString() as test_string
    """

}
