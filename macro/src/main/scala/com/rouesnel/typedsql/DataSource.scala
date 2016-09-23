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
import com.rouesnel.typedsql.core._
import com.rouesnel.typedsql.hive.{HiveMetadataTable, QueryRewriter}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
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
                    tableName: DataSource[_, _] => String,
                    viewName: DataSource[_, _] => String,
                    hdfsPath: DataSource[_, _] => String) {}

  def randomPositive = math.abs(Random.nextLong())
  def timeFormat     = new SimpleDateFormat("yyyyMMddHHmmss")
  def currentTime    = timeFormat.format(new Date())

  def defaultConfig(conf: HiveConf, args: Map[String, List[String]]) =
    Config(
      conf,
      args,
      src => s"typedsql_tmp.${currentTime}_${randomPositive}",
      src => s"typedsql_tmp.${currentTime}_${randomPositive}",
      src => s"/tmp/typedsql_tmp/${currentTime}"
    )

  type Strategy[T <: ThriftStruct, P] = (DataSource.Config,
                                         PersistableSource[T, P]) => Execution[DataSource[T, P]]
  object Strategy {
    def alwaysRefresh[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
        name: String,
        hiveTable: String,
        path: String): Strategy[T, P] =
      (config: Config, underlying: PersistableSource[T, P]) => {
        val named: DataSource[T, P] = underlying match {
          case hq: HiveQueryDataSource[T, P] =>
            hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
          case tp: TypedPipeDataSource[T, P] =>
            tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        }

        Execution.from {
          val db :: tableName :: Nil = hiveTable.split("\\.").toList

          val absolutePath = Hdfs
            .mkdirs(Hdfs.path(path))
            .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
            .run(config.conf)
            .foldMessage[Path](
              identity _,
              err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

          val alreadyExists = Hive
            .existsTable(db, tableName)
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

          if (alreadyExists) {
            log.info(
              s"DataSource ${name} already exists (${hiveTable} - ${path}). It is configured to always refresh. Deleting existing tables and materialising data source.")

            Hive
              .withClient(_.dropTable(db, tableName))
              .run(config.conf)
              .fold[Unit](
                identity _,
                err =>
                  throw new Exception(s"Failed when deleting existing table ${hiveTable}: ${err}"))
          } else {
            log.info(
              s"DataSource ${name} does not exist (${hiveTable} - ${path}). Materialising data source (set to always refresh).")
          }
          named
        }
      }

    def reuseExisting[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
        name: String,
        hiveTable: String,
        path: String): Strategy[T, P] =
      (config: Config, underlying: PersistableSource[T, P]) => {
        val named: DataSource[T, P] = underlying match {
          case hq: HiveQueryDataSource[T, _] =>
            hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
          case tp: TypedPipeDataSource[T, _] =>
            tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        }
        Execution.from {
          val db :: tableName :: Nil = hiveTable.split("\\.").toList

          val absolutePath = Hdfs
            .mkdirs(Hdfs.path(path))
            .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
            .run(config.conf)
            .foldMessage[Path](
              identity _,
              err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

          val conflictingTableExists = Hive
            .existsTable(db, tableName)
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

          val alreadyExists = HiveMetadataTable
            .existsTableStrict[T, P](db, tableName, Some(absolutePath))
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(
                  s"Failed when checking whether ${hiveTable} matched the expected table: ${err}"))

          if (alreadyExists && conflictingTableExists) {
            log.info(
              s"DataSource ${name} already exists (${hiveTable} - ${path}) - reusing existing data.")
            MaterialisedHiveTable[T, P](path, hiveTable)
          } else if (conflictingTableExists) {
            log.info(
              s"A conflicting table for DataSource ${name} already exists (${hiveTable} - ${path}). Dropping existing data and rematerialising.")
            Hive
              .withClient(_.dropTable(db, tableName))
              .run(config.conf)
              .fold[Unit](
                identity _,
                err =>
                  throw new Exception(s"Failed when deleting existing table ${hiveTable}: ${err}"))

            named
          } else {
            log.info(
              s"DataSource ${name} does not exist (${hiveTable} - ${path}). Materialising data source.")
            named
          }
        }
      }

    def appendToExisting[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
        name: String,
        hiveTable: String,
        path: String,
        recreateOnIncompatibleSchema: Boolean = false): Strategy[T, P] =
      (config: Config, underlying: PersistableSource[T, P]) => {
        val named: DataSource[T, P] = underlying match {
          case hq: HiveQueryDataSource[T, _] =>
            hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
          case tp: TypedPipeDataSource[T, _] =>
            tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        }
        Execution.from {
          val db :: tableName :: Nil = hiveTable.split("\\.").toList

          val absolutePath = Hdfs
            .mkdirs(Hdfs.path(path))
            .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
            .run(config.conf)
            .foldMessage[Path](
              identity _,
              err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

          val conflictingTableExists = Hive
            .existsTable(db, tableName)
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

          val alreadyExists = HiveMetadataTable
            .existsTableStrict[T, P](db, tableName, Some(absolutePath))
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(
                  s"Failed when checking whether ${hiveTable} matched the expected table: ${err}"))

          if (alreadyExists && conflictingTableExists) {
            log.info(
              s"DataSource ${name} already exists (${hiveTable} - ${path}) - will append/overwrite partitions.")
            named
          } else if (conflictingTableExists) {
            if (recreateOnIncompatibleSchema) {
              log.info(
                s"A conflicting table for DataSource ${name} already exists (${hiveTable} - ${path}). Dropping existing data and rematerialising.")
              Hive
                .withClient(_.dropTable(db, tableName))
                .run(config.conf)
                .fold[Unit](identity _,
                            err =>
                              throw new Exception(
                                s"Failed when deleting existing table ${hiveTable}: ${err}"))

              named
            } else {
              log.error(
                s"A conflicting table for DataSource ${name} already exists (${hiveTable} - ${path}). Aborting.")

              throw new Exception(s"Table with incompatible schema exists: ${hiveTable}")
            }
          } else {
            log.info(
              s"DataSource ${name} does not exist (${hiveTable} - ${path}). Materialising data source.")
            named
          }
        }
      }

    def forceReuseExisting[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
        name: String,
        hiveTable: String,
        path: String): Strategy[T, _] =
      (config: Config, underlying: PersistableSource[T, P]) => {
        val named: DataSource[T, P] = underlying match {
          case hq: HiveQueryDataSource[T, _] =>
            hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
          case tp: TypedPipeDataSource[T, _] =>
            tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        }
        Execution.from {
          val db :: tableName :: Nil = hiveTable.split("\\.").toList

          val absolutePath = Hdfs
            .mkdirs(Hdfs.path(path))
            .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
            .run(config.conf)
            .foldMessage[Path](
              identity _,
              err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

          val conflictingTableExists = Hive
            .existsTable(db, tableName)
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

          val alreadyExists = HiveMetadataTable
            .existsTableStrict[T, P](db, tableName, Some(absolutePath))
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(
                  s"Failed when checking whether ${hiveTable} matched the expected table: ${err}"))

          if (alreadyExists && conflictingTableExists) {
            log.info(
              s"DataSource ${name} already exists (${hiveTable} - ${path}) - reusing existing data.")
            MaterialisedHiveTable[T, P](path, hiveTable)
          } else if (conflictingTableExists) {
            log.warn(
              s"A conflicting table for DataSource ${name} already exists (${hiveTable} - ${path}). Forcing reuse of existing table (may break!).")
            MaterialisedHiveTable[T, P](path, hiveTable)
          } else {
            log.info(
              s"DataSource ${name} does not exist (${hiveTable} - ${path}). Materialising data source.")
            named
          }
        }
      }

    def flaggedReuse[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
        name: String,
        hiveTable: String,
        path: String): Strategy[T, P] =
      (config: Config, underlying: PersistableSource[T, P]) => {
        val named: DataSource[T, P] = underlying match {
          case hq: HiveQueryDataSource[_, _] =>
            hq.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
          case tp: TypedPipeDataSource[_, _] =>
            tp.copy(hiveTableName = Some(hiveTable), hdfsPath = Some(path))
        }

        Execution.from {
          val db :: tableName :: Nil = hiveTable.split("\\.").toList

          val absolutePath = Hdfs
            .mkdirs(Hdfs.path(path))
            .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
            .run(config.conf)
            .foldMessage[Path](
              identity _,
              err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

          val conflictingTableExists = Hive
            .existsTable(db, tableName)
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(s"Failed when checking whether ${hiveTable} existed: ${err}"))

          val alreadyExists = HiveMetadataTable
            .existsTableStrict[T, P](db, tableName, Some(absolutePath))
            .run(config.conf)
            .fold[Boolean](
              identity _,
              err =>
                throw new Exception(
                  s"Failed when checking whether ${hiveTable} matched the expected table: ${err}"))

          if (alreadyExists && config.args.contains(s"reuse-${name}")) {
            log.info(
              s"DataSource ${name} already exists (${hiveTable} - ${path}) - reusing existing data.")
            MaterialisedHiveTable[T, P](path, hiveTable)
          } else {
            if (conflictingTableExists) {
              log.info(
                s"DataSource ${name} already exists (${hiveTable} - ${path}) even though --reuse-${name} flag is not set. Deleting existing tables and materialising data source.")

              Hive
                .withClient(_.dropTable(db, tableName))
                .run(config.conf)
                .fold[Unit](identity _,
                            err =>
                              throw new Exception(
                                s"Failed when deleting existing table ${hiveTable}: ${err}"))
            } else {
              log.info(
                s"DataSource ${name} does not exist (${hiveTable} - ${path}) even though --reuse-${name} flag is set. Materialising data source.")
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
sealed abstract class DataSource[T <: ThriftStruct, P] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]]
  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T, P]]
}

/**
  * A source that can be represented/used as a Hive view (e.g. does not require materialisation).
  */
sealed trait HiveViewSource[T <: ThriftStruct, P] {
  def toHiveView(config: DataSource.Config): Execution[HiveView[T, P]]
}

sealed trait PersistableSource[T <: ThriftStruct, P] { this: DataSource[T, P] =>

  def manifest: Manifest[T]

  def hasStructType: HasStructType[T]

  def persist(strategy: DataSource.Strategy[T, P]): PersistedDataSource[T, P] = {
    implicit val m  = manifest
    implicit val st = hasStructType
    PersistedDataSource[T, P](this, strategy)
  }
}

/**
  * A Hive table that has been persisted/already exists on HDFS.
  *
  * @param hdfsPath path to dataset on HDFS
  * @param hiveTable fully qualified name (e.g. `db.tablename`)
  */
case class MaterialisedHiveTable[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
    hdfsPath: String,
    hiveTable: String)
    extends DataSource[T, P] {

  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    Execution.from({
      val db :: table :: Nil = hiveTable.split("\\.").toList
      val partitions = Hive
        .listPartitions(db, table)
        .run(config.conf)
        .fold(identity,
              err => throw new Exception(s"Error retrieving partitions for ${hiveTable}: ${err}"))

      val srcs = Option(partitions).filter(_.nonEmpty).getOrElse(List(new Path(hdfsPath)))

      TypedPipe.from(ParquetScroogeSource[T](srcs.map(_.toString): _*))
    })

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T, P]] =
    Execution.from(this)
}

/**
  * A Hive view that has been persisted/created.
  *
  * @param hiveTable name in the Hive metastore (including database).
  * @tparam T type of output records.
  */
case class HiveView[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](hiveTable: String)
    extends DataSource[T, P]
    with HiveViewSource[T, P] {
  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    HiveQueryDataSource(s"SELECT * FROM ${hiveTable}", Map.empty, Map.empty, Map.empty)
      .toTypedPipe(config)
  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T, P]] =
    HiveQueryDataSource(s"SELECT * FROM ${hiveTable}", Map.empty, Map.empty, Map.empty)
      .toHiveTable(config)
  def toHiveView(config: DataSource.Config): Execution[HiveView[T, P]] = Execution.from(this)
}

/**
  * Generic Scalding Typed Pipe source. Will be persisted to disk on first use as a Hive table.
  */
case class TypedPipeDataSource[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
    pipe: TypedPipe[T],
    partitions: List[(String, String)] = Nil,
    hiveTableName: Option[String] = None,
    hdfsPath: Option[String] = None
) extends DataSource[T, P]
    with PersistableSource[T, P] {
  def manifest: Manifest[T] = implicitly[Manifest[T]]

  def hasStructType: HasStructType[T] = implicitly[HasStructType[T]]

  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    Execution.from { pipe }

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T, P]] = {
    val path               = hdfsPath.getOrElse(config.hdfsPath(this))
    val tableName          = hiveTableName.getOrElse(config.tableName(this))
    val db :: table :: Nil = tableName.split("\\.").toList
    pipe
      .writeExecution(ParquetScroogeSource[T](path))
      .flatMap(_ =>
        Execution.from {
          DataSource.synchronized {
            HiveMetadataTable
              .createTable[T, P](db, table, Some(new Path(path)))
              .run(config.conf) match {
              case Ok(_) => MaterialisedHiveTable(path, tableName)
              case Error(these) =>
                these.fold(
                  msg => throw new Exception(s"Error creating Hive table: ${msg}"),
                  ex => throw new Exception(s"Error creating Hive table", ex), {
                    case (msg, ex) => throw new Exception(s"Error creating Hive table: $msg", ex)
                  }
                )
            }
          }
      })
  }

}

/**
  * Compiled and parameterised Hive query with defined sources.
  */
case class HiveQueryDataSource[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
    query: String,
    parameters: Map[String, String],
    sources: Map[String, DataSource[_, _]],
    udfs: Map[(String, String), Class[GenericUDF]],
    hiveViewName: Option[String] = None,
    hiveTableName: Option[String] = None,
    hdfsPath: Option[String] = None
) extends DataSource[T, P]
    with HiveViewSource[T, P]
    with PersistableSource[T, P] {

  def manifest: Manifest[T] = implicitly[Manifest[T]]

  def hasStructType: HasStructType[T] = implicitly[HasStructType[T]]
  def structType                      = hasStructType.structType

  def getUpstreamUdfs(): Map[(String, String), Class[GenericUDF]] =
    sources
      .collect({
        case (_, hqds: HiveQueryDataSource[_, _]) => hqds.udfs
      })
      .fold(Map.empty)(_ ++ _)

  def toHiveView(config: DataSource.Config): Execution[HiveView[T, P]] =
    Execution
      .sequence(sources.toList.map({
        case (variableName, hvs: HiveViewSource[_, _]) =>
          hvs.toHiveView(config).map(view => variableName -> view.hiveTable)
        case (variableName, dataSource: DataSource[_, _]) =>
          dataSource
            .toHiveTable(config)
            .map(executedQuery => (variableName, executedQuery.hiveTable))
      }))
      .map(_.toMap)
      .flatMap(evaluatedSources =>
        Execution.from {
          // Secondly run this execution.
          SessionState.start(config.conf)
          SessionState.get().setIsSilent(true)
          val driver = new Driver(config.conf)

          val viewName     = hiveViewName.getOrElse(config.viewName(this))
          val databaseName = viewName.split("\\.").headOption

          SessionState.get().setHiveVariables((parameters ++ evaluatedSources).asJava)

          Option(GenericOptionsParser.getLibJars(config.conf))
            .map(_.toList)
            .getOrElse(Nil)
            .foreach(url => {
              SessionState.registerJar(url.getPath)
            })

          try {
            driver.init()

            val createDatabaseQuery =
              databaseName.map(db => s"CREATE DATABASE IF NOT EXISTS ${db}")
            createDatabaseQuery
              .map(driver.run(_))
              .filter(_.getResponseCode != 0)
              .foreach(resp => {
                throw new Exception(
                  s"Error creating database ${databaseName}. ${resp.getErrorMessage}")
              })

            // Interpolate any variables needed (so it can be parsed correctly).
            val interpolatedQuery = new VariableSubstitution().substitute(config.conf, query)

            // Maps the UDF names to ids instead so they are unique.
            val mappedQuery = QueryRewriter
              .replaceFunctionInvocations(interpolatedQuery, config.conf, udfs.keys.toMap)

            (udfs ++ getUpstreamUdfs).toList.map({
              case ((name, id), clazz) => FunctionRegistry.registerTemporaryGenericUDF(id, clazz)
            })

            val createQuery =
              s"""
             |CREATE VIEW ${viewName}
             |AS ${mappedQuery}
          """.stripMargin

            runQuery(driver)(createQuery, s"Error creating view using query '${query}'")
            HiveView[T, P](viewName)
          } catch {
            case NonFatal(ex) => throw new Exception(s"Error trying to run query '$query'", ex)
          } finally {
            driver.destroy()
            SessionState.detachSession()
          }
      })

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T, P]] =
    Execution
      .sequence(
        // First run all the source executions.
        sources.toList.map({
          case (variableName, hvs: HiveViewSource[_, _]) =>
            hvs.toHiveView(config).map(view => (variableName, view.hiveTable))
          case (variableName, dataSource: DataSource[_, _]) =>
            dataSource
              .toHiveTable(config)
              .map(executedQuery => (variableName, executedQuery.hiveTable))
        }))
      .map(_.toMap)
      .flatMap(evaluatedSources =>
        Execution.from {
          // Secondly run this execution.
          SessionState.start(config.conf)
          SessionState.get().setIsSilent(true)
          val driver = new Driver(config.conf)

          val tableName     = hiveTableName.getOrElse(config.tableName(this))
          val databaseName  = tableName.split("\\.").headOption
          val justTableName = tableName.split("\\.").lastOption.getOrElse(tableName)
          val path          = hdfsPath.getOrElse(config.hdfsPath(this))

          SessionState.get().setHiveVariables((parameters ++ evaluatedSources).asJava)
          SessionState
            .get()
            .setOverriddenConfigurations(
              Map(
                "hive.exec.dynamic.partition"      -> "true",
                "hive.exec.dynamic.partition.mode" -> "nonstrict"
              ).asJava)

          Option(GenericOptionsParser.getLibJars(config.conf))
            .map(_.toList)
            .getOrElse(Nil)
            .foreach(url => {
              SessionState.registerJar(url.getPath)
            })

          try {
            driver.init()

            val createDatabaseQuery =
              databaseName.map(db => s"CREATE DATABASE IF NOT EXISTS ${db}")
            createDatabaseQuery
              .map(driver.run(_))
              .filter(_.getResponseCode != 0)
              .foreach(resp => {
                throw new Exception(
                  s"Error creating database ${databaseName}. ${resp.getErrorMessage}")
              })

            // Interpolate any variables needed (so it can be parsed correctly).
            val interpolatedQuery = new VariableSubstitution().substitute(config.conf, query)

            // Maps the UDF names to ids instead so they are unique.
            val mappedQuery = QueryRewriter
              .replaceFunctionInvocations(interpolatedQuery, config.conf, udfs.keys.toMap)

            (udfs ++ getUpstreamUdfs).toList.map({
              case ((name, id), clazz) => FunctionRegistry.registerTemporaryGenericUDF(id, clazz)
            })

            val absolutePath = Hdfs
              .mkdirs(Hdfs.path(path))
              .flatMap(_ => Hdfs.fileStatus(Hdfs.path(path)).map(_.getPath))
              .run(config.conf)
              .foldMessage[Path](
                identity _,
                err => throw new Exception(s"Error creating HDFS path ${path}: ${err}"))

            val partitionColumns = implicitly[Partitions[P]].fields
            val partitionBy = if (partitionColumns.nonEmpty) {
              s"PARTITIONED BY (${partitionColumns
                .map({ case (name, typ) => s"${name} ${typ.hiveType}" })
                .mkString(", ")})"
            } else {
              ""
            }

            // Create the table first.
            runQuery(driver)(
              s"""
                |CREATE TABLE IF NOT EXISTS ${tableName} (${structType.fields.toList
                   .map({ case (name, typ) => s"${name} ${typ.hiveType}" })
                   .mkString(", ")})
                |        ${partitionBy}
                |        STORED AS PARQUET
                |        LOCATION '${absolutePath}'
                |        TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
              """.stripMargin
            )

            // We need to reorganise the columns so that the partitions columns come last.
            val nonPartitionCols = structType.fields.toList
              .map(_._1)
              .filter(name => !partitionColumns.exists({ case (partName, _) => partName == name }))
            val partitionCols = partitionColumns.map(_._1)
            val sortedCols    = nonPartitionCols ++ partitionCols

            val reorganisedColumns = s"""SELECT ${sortedCols
              .map(name => s"mapped.${name}")
              .mkString(", ")} FROM (${mappedQuery}) AS mapped"""

            val partitionStatement = if (partitionCols.isEmpty) { "" } else {
              s"PARTITION (${partitionCols.mkString(", ")})"
            }

            // Insert the data
            runQuery(driver)(s"""
                |INSERT OVERWRITE TABLE ${tableName} ${partitionStatement} ${reorganisedColumns}
              """.stripMargin)

            // Convert from a managed table to an external table.
            databaseName.foreach(db => {
              runQuery(driver)(s"USE ${db}")
            })
            runQuery(driver)(
              s"ALTER TABLE ${justTableName} SET TBLPROPERTIES('EXTERNAL'='TRUE')",
              s"Error converting table ${tableName} from managed to external"
            )
            MaterialisedHiveTable[T, P](path, tableName)
          } catch {
            case NonFatal(ex) => throw new Exception(s"Error trying to run query '$query'", ex)
          } finally {
            driver.destroy()
            SessionState.detachSession()
          }
      })

  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    toHiveTable(config).flatMap(materialised => materialised.toTypedPipe(config))

  def runQuery(driver: Driver)(query: String, msg: String = "") = {
    val errorMessage = if (msg.isEmpty) {
      s"Error running query '$query'"
    } else msg
    val response = driver.run(query)
    if (response.getResponseCode != 0) {
      throw new Exception(s"${errorMessage}: ${response.getErrorMessage}")
    }
  }
}

/**
  * A Data Source that wraps an underlying implementation and applies a strategy to decide whether
  * to execute the underlying data source or to use a cache/stale data.
  */
case class PersistedDataSource[T <: ThriftStruct: Manifest: HasStructType, P](
    underlying: PersistableSource[T, P],
    strategy: DataSource.Strategy[T, P])
    extends DataSource[T, P] {

  def toTypedPipe(config: DataSource.Config): Execution[TypedPipe[T]] =
    strategy(config, underlying).flatMap(_.toTypedPipe(config))

  def toHiveTable(config: DataSource.Config): Execution[MaterialisedHiveTable[T, P]] =
    strategy(config, underlying).flatMap(_.toHiveTable(config))
}
