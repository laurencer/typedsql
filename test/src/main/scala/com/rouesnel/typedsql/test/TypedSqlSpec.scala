package com.rouesnel.typedsql.test

import java.util.Date

import scala.util.Random
import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec
import com.rouesnel.typedsql.{DataSource, HiveSupport, TypedPipeDataSource}
import com.rouesnel.typedsql.DataSource.Config
import com.twitter.scalding._
import TDsl._
import com.twitter.scalding.typed.IterablePipe
import com.twitter.scrooge.ThriftStruct

abstract class TypedSqlSpec extends ThermometerHiveSpec with ParquetLogging {
  def randomPositive = math.abs(Random.nextLong())

  /** Configuration used to the test */
  def testConfig = Config(
    hiveConf,
    _ => s"typedsql_tmp_${randomPositive}" + "." + new Date().getTime + "_" + randomPositive,
    _ => s"typedsql_tmp_${randomPositive}" + "." + new Date().getTime + "_" + randomPositive,
    _ => s"${testDir.resolve("tmp")}/typedsql_tmp/${new Date().getTime}_${randomPositive}"
  )

  def executeDataSource[T <: ThriftStruct : Manifest](source: DataSource[T]): List[T] = {
    executesSuccessfully[List[T]](
      source
        .toTypedPipe(testConfig)
        .flatMap(tp =>
          // The identity call is needed otherwise the tests fail due to internal Parquet
          // shenanigans.
          tp.map(identity).toIterableExecution
        )
        .map(_.toList)
    )
  }

  def createDataSource[T <: ThriftStruct : Manifest](elements: T*): DataSource[T] = {
    TypedPipeDataSource(IterablePipe[T](elements))
  }
}