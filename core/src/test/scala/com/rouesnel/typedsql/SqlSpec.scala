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
  def query =
    """
      SELECT 1 as intValue,
            1.3 as doubleValue,
            "string" as stringValue,
            map("key", 1.0, "key2", 2) as mapValue,
            struct(1.0, "stringvalue", 0) as structValue,
            named_struct("field_name", 1, "field2", 2) as namedStructValue
    """
  // TODO - array's don't work yet: array(1, 2, 3, 4) as arrayValue

}


class SqlSpec extends ThermometerHiveSpec with ParquetLogging { def is = s2"""
  Basic tests $basic
"""
  import au.com.cba.omnia.beeswax.Hive
  def basic = {

    println(Hive.createParquetTable[Person]("test_db", "test", Nil).run(hiveConf))

    println(Hive.query(
      s"""
        CREATE TABLE test_db.output
        STORED AS PARQUET
        LOCATION '${dir}/test'
        AS ${SqlQueryTypeTest.query}
      """).run(hiveConf))

    println("GENERATED!")
    import com.twitter.scalding.TDsl._
    val result = executesSuccessfully(ParquetScroogeSource[SqlQueryTypeTest.Row](s"${dir}/test")
      .map(a => { println(a); a })
      .toIterableExecution
    )
    println(result)

    ok
  }
}
