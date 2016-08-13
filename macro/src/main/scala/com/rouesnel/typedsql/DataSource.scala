package com.rouesnel.typedsql

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
                   tableName: DataSource[_] => String,
                    viewName: DataSource[_] => String,
                    hdfsPath: DataSource[_] => String
                   )

}

/**
 * A Data Source is something that can be queried to produce a dataset on HDFS either as a
 * Hive table or a Typed Pipe.
 */
sealed abstract class DataSource[T <: ThriftStruct] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]]
  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable]
}

/**
 * Represents a Hive table
 * @param hdfsPath path to dataset on HDFS
 * @param hiveTable fully qualified name (e.g. `db.tablename`)
 */
case class MaterialisedHiveTable(hdfsPath: String, hiveTable: String)

case class HiveView(hiveTable: String)

/**
 * Generic Scalding Typed Pipe source. Will be persisted to disk on first use as a Hive table.
 */
case class TypedPipeDataSource[T <: ThriftStruct : Manifest](pipe: TypedPipe[T]) extends DataSource[T] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    Execution.from { pipe }

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable] = {
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
case class HiveQueryDataSource[T <: ThriftStruct : Manifest](query: String,
                               parameters: Map[String, String],
                               sources: Map[String, DataSource[_]],
                               udfs: Map[String, Class[GenericUDF]]
                              ) extends DataSource[T] {

  def toHiveView(config: DataSource.Config): Execution[HiveView] =
    Execution.sequence(sources.toList.map({
      case (variableName, hq: HiveQueryDataSource[_]) => hq.toHiveView(config).map(view => variableName -> view.hiveTable)
      case (variableName, dataSource) =>
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
          HiveView(viewName)
        }
      } catch {
        case NonFatal(ex) => throw new Exception(s"Error trying to run query '$query'", ex)
      } finally {
        driver.destroy()
        SessionState.detachSession()
      }
    })

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable] =
    Execution.sequence(
      // First run all the source executions.
      sources.toList.map({
        case (variableName, hq: HiveQueryDataSource[_]) => hq.toHiveView(config).map(view => (variableName, view.hiveTable))
        case (variableName, dataSource) => dataSource.toHiveTable(config).map(executedQuery => (variableName, executedQuery.hiveTable))
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
          MaterialisedHiveTable(hdfsPath, tableName)
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