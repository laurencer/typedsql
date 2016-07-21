package com.rouesnel.typedsql

import java.util.Date

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource
import au.com.cba.omnia.thermometer.core.{Thermometer, ThermometerRecordReader}
import Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids.{exists, missing, records}
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec
import com.rouesnel.typedsql.DataSource.Config
import com.twitter.scalding.typed.IterablePipe

import scala.util.Random

@SqlQuery object SqlQueryTypeTest {

  case class Sources(//testStruct: DataSource[NestedStructTest],
                     people: DataSource[com.rouesnel.typedsql.Person],
                     test: DataSource[ManualSqlStruct],
                     myPeople: DataSource[Person]
                    )

  case class Parameters(minimumAge: Int)

  def query =
    """
      SELECT 1 as int_value,
            1.3 as double_value,
            "string" as string_value,
            map("key", 1.0, "key2", 2) as map_value,
            struct(1.0, "stringvalue", 0) as struct_value,
            named_struct("field_name", 1, "field2", 2) as named_struct_value
      FROM ${people}
      WHERE age > ${minimumAge}
    """
  // TODO - array's don't work yet: array(1, 2, 3, 4) as arrayValue

}

@SqlQuery object ChainedTest {

  case class Parameters()

  case class Sources(upstream: DataSource[SqlQueryTypeTest.Row])

  def query =
    """
      SELECT int_value, COUNT(*) as count
      FROM ${upstream}
      GROUP BY int_value
    """
}


class SqlSpec extends ThermometerHiveSpec with ParquetLogging { def is = s2"""
  Basic tests $basic
"""
  def basic = {
    def testConfig = Config(
      hiveConf,
      _ => "typedsql_tmp" + "." + new Date().getTime + "_" + math.abs(Random.nextLong()),
      _ => "typedsql_tmp" + "." + new Date().getTime + "_" + math.abs(Random.nextLong()),
      _ => s"${testDir.resolve("tmp")}/typedsql_tmp/${new Date().getTime}_${math.abs(Random.nextLong())}"
    )


    val sqlDataSource: DataSource[SqlQueryTypeTest.Row] = SqlQueryTypeTest(
      SqlQueryTypeTest.Sources(
        //TypedPipeDataSource(IterablePipe[NestedStructTest](Nil)),
        TypedPipeDataSource(IterablePipe[Person](List(Person("Bob", "Brown", 28)))),
        TypedPipeDataSource(IterablePipe[ManualSqlStruct](Nil)),
        TypedPipeDataSource(IterablePipe[Person](Nil))
      ),
      SqlQueryTypeTest.Parameters(18)
    )

    val chainedDataSource = ChainedTest(
      ChainedTest.Sources(sqlDataSource),
      ChainedTest.Parameters()
    )

    val result = executesSuccessfully[Iterable[SqlQueryTypeTest.Row]](
      sqlDataSource.toTypedPipe(testConfig).flatMap(tp => tp.map(a => {println(a); a}).toIterableExecution)
    )

    val resultChained = executesSuccessfully[Iterable[ChainedTest.Row]](
      chainedDataSource.toTypedPipe(testConfig).flatMap(tp => tp.map(a => {println(a); a}).toIterableExecution)
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
