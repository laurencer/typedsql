package com.rouesnel.typedsql

import java.text.SimpleDateFormat
import java.util.{ArrayList, Date}

import au.com.cba.omnia.beeswax.Hive
import au.com.cba.omnia.omnitool._
import com.twitter.scalding.{Execution, TypedPipe}
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF

import scala.util.Random
import scala.util.control.NonFatal
import scala.collection.convert.decorateAsJava._
import scalaz.\/

object DataSource {
  case class Config(conf: HiveConf,
                    tableName: DataSource[_]  => String,
                    viewName:  DataSource[_]  => String,
                    hdfsPath:  DataSource[_]  => String,
                    namedSources: Map[DataSource[_], String] = Map.empty
                   ) {

  }

  def randomPositive = math.abs(Random.nextLong())
  def timeFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  def currentTime = timeFormat.format(new Date())

  def defaultConfig(conf: HiveConf) =
    Config(
      conf,
      src => s"typedsql_tmp.${currentTime}_${randomPositive}",
      src => s"typedsql_tmp.${currentTime}_${randomPositive}",
      src => s"/tmp/typedsql_tmp/${currentTime}"
    )
}

/**
 * A Data Source is something that can be queried to produce a dataset on HDFS either as a
 * Hive table or a Typed Pipe.
 */
sealed abstract class DataSource[T <: ThriftStruct] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]]
  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]]
}

/**
 * A source that can be represented/used as a Hive view (e.g. does not require materialisation).
 */
trait HiveViewSource[T <: ThriftStruct] {
  def toHiveView(config: DataSource.Config): Execution[HiveView[T]]
}

  /**
 * A Hive table that has been persisted/already exists on HDFS.
   *
   * @param hdfsPath path to dataset on HDFS
 * @param hiveTable fully qualified name (e.g. `db.tablename`)
 */
case class MaterialisedHiveTable[T <: ThriftStruct : Manifest](hdfsPath: String, hiveTable: String) extends DataSource[T] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    Execution.from({
      val db :: table :: Nil = hiveTable.split("\\.").toList
      val partitions = Hive
        .listPartitions(db, table)
        .run(config.conf)
        .fold(identity, err => throw new Exception(s"Error retrieving partitions for ${hiveTable}: ${err}"))

      TypedPipe.from(ParquetScroogeSource[T](partitions.map(_.toString): _*))
    })

    def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]]
    = Execution.from(this)
}

/**
 * A Hive view that has been persisted/created.
 *
 * @param hiveTable name in the Hive metastore (including database).
 * @tparam T type of output records.
 */
case class HiveView[T <: ThriftStruct : Manifest](hiveTable: String) extends DataSource[T] with HiveViewSource[T] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    HiveQueryDataSource(s"SELECT * FROM ${hiveTable}", Map.empty, Map.empty, Map.empty).toTypedPipe(config)
  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]] =
    HiveQueryDataSource(s"SELECT * FROM ${hiveTable}", Map.empty, Map.empty, Map.empty).toHiveTable(config)
  def toHiveView(config: DataSource.Config): Execution[HiveView[T]] = Execution.from(this)
}

/**
 * Generic Scalding Typed Pipe source. Will be persisted to disk on first use as a Hive table.
 */
case class TypedPipeDataSource[T <: ThriftStruct : Manifest](pipe: TypedPipe[T]) extends DataSource[T] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    Execution.from { pipe }

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]] = {
    val path  = config.hdfsPath(this)
    val tableName = config.tableName(this)
    val db :: table :: Nil = tableName.split("\\.").toList
    pipe.writeExecution(ParquetScroogeSource[T](path)).flatMap(_ => Execution.from {
      DataSource.synchronized {
        Hive.createParquetTable[T](db, table, Nil, Some(new Path(path))).run(config.conf) match {
          case Ok(_)        => MaterialisedHiveTable(path, tableName)
          case Error(these) => throw new Exception(s"Error creating Hive table: ${these}")
        }
      }
    })
  }
}

/**
 * Compiled and parameterised Hive query with defined sources.
 */
case class HiveQueryDataSource[T <: ThriftStruct : Manifest](
                               query: String,
                               parameters: Map[String, String],
                               sources: Map[String, DataSource[_]],
                               udfs: Map[String, Class[GenericUDF]]
                              ) extends DataSource[T] {

  def toHiveView(config: DataSource.Config): Execution[HiveView[T]] =
    Execution.sequence(sources.toList.map({
      case (variableName, hvs:  HiveViewSource[_]) => hvs.toHiveView(config).map(view => variableName -> view.hiveTable)
      case (variableName, dataSource: DataSource[_]) =>
        dataSource.toHiveTable(config).map(executedQuery => (variableName, executedQuery.hiveTable))
    })).map(_.toMap).flatMap(evaluatedSources => Execution.from {
      // Secondly run this execution.
      SessionState.start(config.conf)
      SessionState.get().setIsSilent(true)
      val driver = new Driver(config.conf)

      val viewName      = config.viewName(this)
      val databaseName  = viewName.split("\\.").headOption
      val hdfsPath      = config.hdfsPath(this)

      SessionState.get().setHiveVariables((parameters ++ evaluatedSources).asJava)

      try {
        driver.init()

        val createDatabaseQuery = databaseName.map(db => s"CREATE DATABASE IF NOT EXISTS ${db}")
        createDatabaseQuery.map(driver.run(_)).filter(_.getResponseCode != 0).foreach(resp => {
          throw new Exception(s"Error creating database ${databaseName}. ${resp.getErrorMessage}")
        })

        udfs.toList.map({ case (name, clazz) => FunctionRegistry.registerTemporaryGenericUDF(name, clazz) })

        val createQuery =
          s"""
             |CREATE VIEW ${viewName}
             |AS ${query}
          """.stripMargin

        val response = driver.run(createQuery)
        if (response.getResponseCode() != 0)
          throw new Exception(s"Error creating view using query '$query'. ${response.getErrorMessage}")
        else {
          HiveView[T](viewName)
        }
      } catch {
        case NonFatal(ex) => throw new Exception(s"Error trying to run query '$query'", ex)
      } finally {
        driver.destroy()
        SessionState.detachSession()
      }
    })

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]] =
    Execution.sequence(
      // First run all the source executions.
      sources.toList.map({
        case (variableName, hvs: HiveViewSource[_]) => hvs.toHiveView(config).map(view => (variableName, view.hiveTable))
        case (variableName, dataSource: DataSource[_]) => dataSource.toHiveTable(config).map(executedQuery => (variableName, executedQuery.hiveTable))
    })).map(_.toMap).flatMap(evaluatedSources => Execution.from {
      // Secondly run this execution.
      SessionState.start(config.conf)
      SessionState.get().setIsSilent(true)
      val driver = new Driver(config.conf)

      val tableName    = config.tableName(this)
      val databaseName = tableName.split("\\.").headOption
      val hdfsPath     = config.hdfsPath(this)

      SessionState.get().setHiveVariables((parameters ++ evaluatedSources).asJava)

      try {
        driver.init()

        val createDatabaseQuery = databaseName.map(db => s"CREATE DATABASE IF NOT EXISTS ${db}")
        createDatabaseQuery.map(driver.run(_)).filter(_.getResponseCode != 0).foreach(resp => {
          throw new Exception(s"Error creating database ${databaseName}. ${resp.getErrorMessage}")
        })

        udfs.toList.map({ case (name, clazz) => FunctionRegistry.registerTemporaryGenericUDF(name, clazz) })

        val createQuery =
          s"""
             |CREATE TABLE ${tableName}
             |        STORED AS PARQUET
             |        LOCATION '${hdfsPath}'
             |        AS ${query}
          """.stripMargin
        val response = driver.run(createQuery)
        if (response.getResponseCode() != 0)
          throw new Exception(s"Error running query '$query'. ${response.getErrorMessage}")
        else {
          MaterialisedHiveTable[T](hdfsPath, tableName)
        }
      } catch {
        case NonFatal(ex) => throw new Exception(s"Error trying to run query '$query'", ex)
      } finally {
        driver.destroy()
        SessionState.detachSession()
      }
    })

  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    toHiveTable(config).map(materialised => {
      TypedPipe.from(ParquetScroogeSource[T](materialised.hdfsPath))
    })
}

/**
 * A Data Source that wraps an underlying implementation and applies a strategy to decide whether
 * to execute the underlying data source or to use a cache/stale data.
 */
case class PersistedDataSource[T <: ThriftStruct : Manifest](underlying: DataSource[T],
                                                             strategy: (DataSource.Config, DataSource[T]) => Execution[DataSource[T]]
                                                         ) extends DataSource[T] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    strategy(config, underlying).flatMap(_.toTypedPipe(config))

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]] =
    strategy(config, underlying).flatMap(_.toHiveTable(config))
}