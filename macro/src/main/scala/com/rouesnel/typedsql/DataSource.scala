package com.rouesnel.typedsql

import java.text.SimpleDateFormat
import java.util.Date

import com.twitter.scalding.{Execution, TypedPipe}
import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.log4j.Logger
import au.com.cba.omnia.beeswax.Hive
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource
import au.com.cba.omnia.omnitool._
import au.com.cba.omnia.permafrost.hdfs.Hdfs
import com.rouesnel.typedsql.core.StructType
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.util.GenericOptionsParser

import scala.util.Random
import scala.util.control.NonFatal
import scala.collection.convert.decorateAsJava._
import scalaz._
import Scalaz._

object DataSource {

  val log = LogFactory.getLog(getClass)

  case class Config(conf: HiveConf,
                    args: Map[String, List[String]],
                    tableName: DataSource[_]  => String,
                    viewName:  DataSource[_]  => String,
                    hdfsPath:  DataSource[_]  => String
                   ) {

  }

  def randomPositive = math.abs(Random.nextLong())
  def timeFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  def currentTime = timeFormat.format(new Date())

  def defaultConfig(conf: HiveConf, args: Map[String, List[String]]) =
    Config(
      conf,
      args,
      src => s"typedsql_tmp.${currentTime}_${randomPositive}",
      src => s"typedsql_tmp.${currentTime}_${randomPositive}",
      src => s"/tmp/typedsql_tmp/${currentTime}"
    )

  type Strategy[T <: ThriftStruct] = (DataSource.Config, PersistableSource[T]) => Execution[DataSource[T]]
  object Strategy {
    def alwaysRefresh[T <: ThriftStruct : Manifest : HasStructType](name: String, hiveTable: String, path: String): Strategy[T] = (config: Config, underlying: PersistableSource[T]) => {
      val named = underlying match {
        case hq: HiveQueryDataSource[_] => hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        case tp: TypedPipeDataSource[_] => tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
      }

      Execution.from {
        val db :: tableName :: Nil = hiveTable.split("\\.").toList

        val absolutePath = Hdfs.mkdirs(Hdfs.path(path))
          .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
          .run(config.conf)
          .foldMessage[Path](identity _, err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

        val alreadyExists = Hive.existsTable(db, tableName).run(config.conf)
          .fold[Boolean](identity _, err => throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

        if (alreadyExists) {
          log.info(s"DataSource ${name} already exists (${hiveTable} - ${path}). It is configured to always refresh. Deleting existing tables and materialising data source.")

          Hive.withClient(_.dropTable(db, tableName))
            .run(config.conf)
            .fold[Unit](identity _, err => throw new Exception(s"Failed when deleting existing table ${hiveTable}: ${err}"))
        } else {
          log.info(s"DataSource ${name} does not exist (${hiveTable} - ${path}). Materialising data source (set to always refresh).")
        }
        named
      }
    }

    def reuseExisting[T <: ThriftStruct : Manifest : HasStructType](name: String, hiveTable: String, path: String): Strategy[T] = (config: Config, underlying: PersistableSource[T]) => {
      val named = underlying match {
        case hq: HiveQueryDataSource[T] => hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        case tp: TypedPipeDataSource[T] => tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
      }
      Execution.from {
        val db :: tableName :: Nil = hiveTable.split("\\.").toList

        val absolutePath = Hdfs.mkdirs(Hdfs.path(path))
          .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
          .run(config.conf)
          .foldMessage[Path](identity _, err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

        val conflictingTableExists = Hive.existsTable(db, tableName).run(config.conf)
          .fold[Boolean](identity _, err => throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

        val alreadyExists = HiveMetadataTable.existsTableStrict[T](db, tableName, underlying.partitions, Some(absolutePath))
          .run(config.conf)
          .fold[Boolean](identity _, err => throw new Exception(s"Failed when checking whether ${hiveTable} matched the expected table: ${err}"))

        if (alreadyExists && conflictingTableExists) {
          log.info(s"DataSource ${name} already exists (${hiveTable} - ${path}) - reusing existing data.")
          MaterialisedHiveTable[T](path, hiveTable)
        } else if (conflictingTableExists) {
          log.info(s"A conflicting table for DataSource ${name} already exists (${hiveTable} - ${path}). Dropping existing data and rematerialising.")
          Hive.withClient(_.dropTable(db, tableName))
            .run(config.conf)
            .fold[Unit](identity _, err => throw new Exception(s"Failed when deleting existing table ${hiveTable}: ${err}"))

          named
        } else {
          log.info(s"DataSource ${name} does not exist (${hiveTable} - ${path}). Materialising data source.")
          named
        }
      }
    }

    def forceReuseExisting[T <: ThriftStruct : Manifest : HasStructType](name: String, hiveTable: String, path: String): Strategy[T] = (config: Config, underlying: PersistableSource[T]) => {
      val named = underlying match {
        case hq: HiveQueryDataSource[T] => hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        case tp: TypedPipeDataSource[T] => tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
      }
      Execution.from {
        val db :: tableName :: Nil = hiveTable.split("\\.").toList

        val absolutePath = Hdfs.mkdirs(Hdfs.path(path))
          .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
          .run(config.conf)
          .foldMessage[Path](identity _, err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

        val conflictingTableExists = Hive.existsTable(db, tableName).run(config.conf)
          .fold[Boolean](identity _, err => throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

        val alreadyExists = HiveMetadataTable.existsTableStrict[T](db, tableName, underlying.partitions, Some(absolutePath))
          .run(config.conf)
          .fold[Boolean](identity _, err => throw new Exception(s"Failed when checking whether ${hiveTable} matched the expected table: ${err}"))

        if (alreadyExists && conflictingTableExists) {
          log.info(s"DataSource ${name} already exists (${hiveTable} - ${path}) - reusing existing data.")
          MaterialisedHiveTable[T](path, hiveTable)
        } else if (conflictingTableExists) {
          log.warn(s"A conflicting table for DataSource ${name} already exists (${hiveTable} - ${path}). Forcing reuse of existing table (may break!).")
          MaterialisedHiveTable[T](path, hiveTable)
        } else {
          log.info(s"DataSource ${name} does not exist (${hiveTable} - ${path}). Materialising data source.")
          named
        }
      }
    }

    def flaggedReuse[T <: ThriftStruct : Manifest : HasStructType](name: String, hiveTable: String, path: String): Strategy[T] = (config: Config, underlying: PersistableSource[T]) => {
      val named = underlying match {
        case hq: HiveQueryDataSource[_] => hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        case tp: TypedPipeDataSource[_] => tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
      }

      Execution.from {
        val db :: tableName :: Nil = hiveTable.split("\\.").toList

        val absolutePath = Hdfs.mkdirs(Hdfs.path(path))
          .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
          .run(config.conf)
          .foldMessage[Path](identity _, err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

        val conflictingTableExists = Hive.existsTable(db, tableName).run(config.conf)
          .fold[Boolean](identity _, err => throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

        val alreadyExists = HiveMetadataTable.existsTableStrict[T](db, tableName, underlying.partitions, Some(absolutePath))
          .run(config.conf)
          .fold[Boolean](identity _, err => throw new Exception(s"Failed when checking whether ${hiveTable} matched the expected table: ${err}"))

        if (alreadyExists && config.args.contains(s"reuse-${name}")) {
          log.info(s"DataSource ${name} already exists (${hiveTable} - ${path}) - reusing existing data.")
          MaterialisedHiveTable[T](path, hiveTable)
        } else {
          if (conflictingTableExists) {
            log.info(s"DataSource ${name} already exists (${hiveTable} - ${path}) even though --reuse-${name} flag is not set. Deleting existing tables and materialising data source.")

            Hive.withClient(_.dropTable(db, tableName))
              .run(config.conf)
              .fold[Unit](identity _, err => throw new Exception(s"Failed when deleting existing table ${hiveTable}: ${err}"))
          } else {
            log.info(s"DataSource ${name} does not exist (${hiveTable} - ${path}) even though --reuse-${name} flag is set. Materialising data source.")
          }
          named
        }
      }
    }
  }
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
sealed trait HiveViewSource[T <: ThriftStruct] {
  def toHiveView(config: DataSource.Config): Execution[HiveView[T]]
}

sealed trait PersistableSource[T <: ThriftStruct] {
  this: DataSource[T] =>

  def manifest: Manifest[T]

  def hasStructType: HasStructType[T]

  def partitions: List[(String, String)]

  def persist(strategy: DataSource.Strategy[T]): PersistedDataSource[T] = {
    implicit val m  = manifest
    implicit val st = hasStructType
    PersistedDataSource[T](this, strategy)
  }
}

/**
 * A Hive table that has been persisted/already exists on HDFS.
 *
 * @param hdfsPath path to dataset on HDFS
 * @param hiveTable fully qualified name (e.g. `db.tablename`)
 */
case class MaterialisedHiveTable[T <: ThriftStruct : Manifest : HasStructType](hdfsPath: String,
                                                               hiveTable: String,
                                                               partitions: List[(String, String)] = Nil
                                                              ) extends DataSource[T] {
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
case class HiveView[T <: ThriftStruct : Manifest : HasStructType](hiveTable: String) extends DataSource[T] with HiveViewSource[T] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    HiveQueryDataSource(s"SELECT * FROM ${hiveTable}", Map.empty, Map.empty, Map.empty).toTypedPipe(config)
  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]] =
    HiveQueryDataSource(s"SELECT * FROM ${hiveTable}", Map.empty, Map.empty, Map.empty).toHiveTable(config)
  def toHiveView(config: DataSource.Config): Execution[HiveView[T]] = Execution.from(this)
}

/**
 * Generic Scalding Typed Pipe source. Will be persisted to disk on first use as a Hive table.
 */
case class TypedPipeDataSource[T <: ThriftStruct : Manifest : HasStructType](
                                                             pipe: TypedPipe[T],
                                                             partitions: List[(String, String)] = Nil,
                                                             hiveTableName: Option[String] = None,
                                                             hdfsPath: Option[String] = None
                                                            ) extends DataSource[T] with PersistableSource[T] {
  def manifest: Manifest[T] = implicitly[Manifest[T]]

  def hasStructType: HasStructType[T] = implicitly[HasStructType[T]]

  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
      Execution.from { pipe }

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]] = {
    val path  = hdfsPath.getOrElse(config.hdfsPath(this))
    val tableName = hiveTableName.getOrElse(config.tableName(this))
    val db :: table :: Nil = tableName.split("\\.").toList
    pipe.writeExecution(ParquetScroogeSource[T](path)).flatMap(_ => Execution.from {
      DataSource.synchronized {
        HiveMetadataTable.createTable[T](db, table, Nil, Some(new Path(path))).run(config.conf) match {
          case Ok(_)        => MaterialisedHiveTable(path, tableName)
          case Error(these) => these.fold(
            msg   => throw new Exception(s"Error creating Hive table: ${msg}"),
            ex    => throw new Exception(s"Error creating Hive table", ex),
            { case (msg, ex) => throw new Exception(s"Error creating Hive table: $msg", ex) }
          )
        }
      }
    })
  }



}

/**
 * Compiled and parameterised Hive query with defined sources.
 */
case class HiveQueryDataSource[T <: ThriftStruct : Manifest : HasStructType](
                               query: String,
                               parameters: Map[String, String],
                               sources: Map[String, DataSource[_]],
                               udfs: Map[String, Class[GenericUDF]],
                               partitions: List[(String, String)] = Nil,
                               hiveViewName: Option[String] = None,
                               hiveTableName: Option[String] = None,
                               hdfsPath: Option[String] = None
                              ) extends DataSource[T] with HiveViewSource[T] with PersistableSource[T] {

  def manifest: Manifest[T] = implicitly[Manifest[T]]

  def hasStructType: HasStructType[T] = implicitly[HasStructType[T]]

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

      val viewName      = hiveViewName.getOrElse(config.viewName(this))
      val databaseName  = viewName.split("\\.").headOption

      SessionState.get().setHiveVariables((parameters ++ evaluatedSources).asJava)

      Option(GenericOptionsParser.getLibJars(config.conf))
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(url => {
          SessionState.registerJar(url.getPath)
        })

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

      val tableName     = hiveTableName.getOrElse(config.tableName(this))
      val databaseName  = tableName.split("\\.").headOption
      val justTableName = tableName.split("\\.").lastOption.getOrElse(tableName)
      val path          = hdfsPath.getOrElse(config.hdfsPath(this))

      SessionState.get().setHiveVariables((parameters ++ evaluatedSources).asJava)

      Option(GenericOptionsParser.getLibJars(config.conf))
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(url => {
          SessionState.registerJar(url.getPath)
        })

      try {
        driver.init()

        val createDatabaseQuery = databaseName.map(db => s"CREATE DATABASE IF NOT EXISTS ${db}")
        createDatabaseQuery.map(driver.run(_)).filter(_.getResponseCode != 0).foreach(resp => {
          throw new Exception(s"Error creating database ${databaseName}. ${resp.getErrorMessage}")
        })

        udfs.toList.map({ case (name, clazz) => FunctionRegistry.registerTemporaryGenericUDF(name, clazz) })

        val absolutePath = Hdfs.mkdirs(Hdfs.path(path))
          .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
          .run(config.conf)
          .foldMessage[Path](identity _, err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

        // Table must be created as managed so we can use CTAS (create table as select). After
        // executing we change it to an external table.
        val createQuery =
          s"""
             |CREATE TABLE ${tableName}
             |        STORED AS PARQUET
             |        LOCATION '${absolutePath}'
             |        TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
             |        AS ${query}
          """.stripMargin
        val response = driver.run(createQuery)
        if (response.getResponseCode() != 0)
          throw new Exception(s"Error running query '$query'. ${response.getErrorMessage}")
        else {
          // Convert from a managed table to an external table.
          databaseName.foreach(db => {
            driver.run(s"USE ${db}")
            if (response.getResponseCode() != 0)
              throw new Exception(s"Error selecting database ${db}. ${response.getErrorMessage}")
          })
          driver.run(s"ALTER TABLE ${justTableName} SET TBLPROPERTIES('EXTERNAL'='TRUE')")
          if (response.getResponseCode() != 0)
            throw new Exception(s"Error converting table ${tableName} from managed to external. ${response.getErrorMessage}")

          MaterialisedHiveTable[T](path, tableName)
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
case class PersistedDataSource[T <: ThriftStruct : Manifest : HasStructType](underlying: PersistableSource[T],
                                                             strategy: DataSource.Strategy[T]
                                                         ) extends DataSource[T] {

  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    strategy(config, underlying).flatMap(_.toTypedPipe(config))

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T]] =
    strategy(config, underlying).flatMap(_.toHiveTable(config))
}