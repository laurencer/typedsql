package com.rouesnel.typedsql

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource
import com.rouesnel.typedsql.Person
import com.twitter.scalding.Job
import org.joda.time.DateTime
import org.specs2._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen.alphaStr
import org.scalacheck.Prop.forAll

import scala.util.Failure
import au.com.cba.omnia.thermometer.core.{Thermometer, ThermometerRecordReader}, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids.{exists, missing, records}
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

@SqlQuery object SqlQueryTypeTest {
  def sources = Map(
    "test_struct" -> NestedStructTest,
    "people"      -> com.rouesnel.typedsql.Person,
    "test"        -> ManualSqlStruct,
    "my_people"   -> Person
  )

  def query =
    """
      SELECT 1 as int_value,
            1.3 as double_value,
            "string" as string_value,
            map("key", 1.0, "key2", 2) as map_value,
            struct(1.0, "stringvalue", 0) as struct_value,
            named_struct("field_name", 1, "field2", 2) as named_struct_value
    """
  // TODO - array's don't work yet: array(1, 2, 3, 4) as arrayValue

}

@SqlQuery object ChainedTest {
  def sources = Map(
    "upstream" -> SqlQueryTypeTest.Row
  )

  def query =
    """
      SELECT int_value, COUNT(*) as count
      FROM ${upstream}
      GROUP BY int_value
    """
  // TODO - array's don't work yet: array(1, 2, 3, 4) as arrayValue

}


class SqlSpec extends ThermometerHiveSpec with ParquetLogging { def is = s2"""
  Basic tests $basic
"""
  import au.com.cba.omnia.beeswax.Hive
  def basic = {
    println(Hive.createDatabase("test_db").run(hiveConf))

    // Create the environment tables.
    Hive.createParquetTable[Person]("test_db", "p", Nil).run(hiveConf)
    Hive.createParquetTable[ManualSqlStruct]("test_db", "t", Nil).run(hiveConf)
    Hive.createParquetTable[Person]("test_db", "mp", Nil).run(hiveConf)

    val substitutionPattern = "\\$\\{[^\\}\\$\u0020]+\\}".r

    def substitute(query: String): String = {
      substitutionPattern.replaceSomeIn(query, variable => {
        variable.matched match {
          case "${people}" => Some("test_db.p")
          case _  => None
        }
      })
    }

    println(Hive.query(
      s"""
        CREATE TABLE test_db.output
        STORED AS PARQUET
        LOCATION '${dir}/test'
        AS ${substitute(SqlQueryTypeTest.query)}
      """).run(hiveConf))


    println("GENERATED!")

    import com.twitter.scalding.TDsl._
    val result = executesSuccessfully(ParquetScroogeSource[SqlQueryTypeTest.Row](s"${dir}/test")
      .map(a => { println(a); a })
      .toIterableExecution
    )

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
