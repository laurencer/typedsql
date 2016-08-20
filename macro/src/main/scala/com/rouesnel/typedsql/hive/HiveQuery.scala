package com.rouesnel.typedsql.hive

import java.util.Date

import com.rouesnel.typedsql.core._
import com.rouesnel.typedsql.udf.{PlaceholderUDF, UdfDescription}
import com.rouesnel.typedsql.util
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.session.SessionState

import scala.language.experimental.macros
import scala.util.control.NonFatal
import scalaz.Scalaz._
import scalaz._

/** Provides helpers for parsing/manipulating Hive queries */
object HiveQuery {
  import scala.collection.convert.decorateAsJava._

  /**
    * Parses a SELECT query using the Hive Parser.
    */
  def parseSelect(query: String): String \/ ASTNode = {
    try {
      val pd = new ParseDriver()
      \/.right(pd.parseSelect(query, null))
    } catch {
      case ex: Exception =>
        \/.left(s"Error parsing the sql query: ${ex.getMessage}")
    }
  }

  /**
    * Compiles a Hive Query and returns the Hive Schema
    *
    * @param hiveConf conf corresponding to a local instance (see HiveSupport)
    * @param sources the other tables/schemas that should be available
    * @param parameterVariables map of parameter names to default values to use for compilation
    * @param query query to compile
    * @return error or the compiled Hive Schema
    */
  def compileQuery(hiveConf: HiveConf,
                   sources: Map[String, StructType],
                   parameterVariables: Map[String, String],
                   udfs: List[UdfDescription],
                   query: String): Throwable \/ Schema =
    HiveSupport.useHiveClassloader {
      val driver = new Driver(hiveConf)
      try {
        compileQuery(driver, hiveConf, sources, parameterVariables, udfs, query)
      } finally {
        driver.close()
        driver.destroy()
      }
    }

  /**
    * Compiles a Hive Query and returns the Hive Schema using the provided driver.
    *
    * This version is available to support long-running operations where the driver is externally
    * managed.
    *
    * @param driver the Hive driver to use for compilation
    * @param hiveConf conf corresponding to a local instance (see HiveSupport)
    * @param sources the other tables/schemas that should be available
    * @param parameterVariables map of parameter names to default values to use for compilation
    * @param query query to compile
    * @return error or the compiled Hive Schema
    */
  def compileQuery(driver: Driver,
                   hiveConf: HiveConf,
                   sources: Map[String, StructType],
                   parameterVariables: Map[String, String],
                   udfs: List[UdfDescription],
                   query: String): Throwable \/ Schema =
    HiveSupport.useHiveClassloader {
      SessionState.start(hiveConf)
      SessionState.get().setIsSilent(true)
      val dbName = s"test_${new Date().getTime}"

      // Create the compilation environment
      createCompilationEnvironment(dbName, hiveConf, sources)

      // Initialise the variable substitution
      val sourceVariables =
        sources.keys.map(tableName => tableName -> s"${dbName}.${tableName}").toMap

      val variables =
        (sourceVariables ++ parameterVariables).asJava

      // Need to synchronize here because the functions and placeholder UDFs are global.
      PlaceholderUDF.synchronized {
        \/.fromTryCatchNonFatal {
          SessionState.get().setHiveVariables(variables)

          PlaceholderUDF
            .configurePlaceholders(udfs)((udf, udfClazz) =>
              FunctionRegistry.registerTemporaryFunction(udf.name, udfClazz))
            .fold(err => throw new Exception(err), identity)

          // Run the query.
          driver.init()
          driver.compile(query)
          val result = driver.getSchema()

          udfs.foreach(udf => FunctionRegistry.unregisterTemporaryUDF(udf.name))

          result
        }
      }
    }

  /**
    * Creates tables in the specified database with the corresponding schema for each source table.
    *
    * Taken/inspired by the corresponding functions in [Beeswax](https://github.com/CommBank/beeswax).
    * See https://github.com/CommBank/beeswax/blob/master/src/main/scala/au/com/cba/omnia/beeswax/HiveMetadataTable.scala
    */
  def createCompilationEnvironment(dbName: String,
                                   hiveConf: HiveConf,
                                   sources: Map[String, StructType]) = {
    import au.com.cba.omnia.beeswax._
    Hive
      .createDatabase(dbName)
      .flatMap(_ => {
        Hive.getConfClient.map({
          case (conf, client) =>
            sources.toList.map({
              case (tableName, tableSchema) => {
                val table = new Table()
                table.setDbName(dbName)
                table.setTableName(tableName)

                val sd = new StorageDescriptor()
                tableSchema.fields.toList.foreach({
                  case (fieldName, fieldType) => {
                    val fieldSchema =
                      new FieldSchema(fieldName,
                                      fieldType.hiveType,
                                      "Table in compilation environment")
                    sd.addToCols(fieldSchema)
                  }
                })

                table.setSd(sd)

                ParquetFormat.applyFormat(table)

                try {
                  client.createTable(table)
                  Hive.value(true)
                } catch {
                  case NonFatal(t) =>
                    Hive.error(
                      s"Failed to create table $tableName in compilation environment (db = ${dbName})",
                      t)
                }
              }
            })
        })
      })
      .run(hiveConf)
  }
}
