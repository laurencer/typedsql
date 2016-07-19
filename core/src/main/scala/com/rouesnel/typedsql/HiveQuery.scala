package com.rouesnel.typedsql

import java.io.File
import java.util.Date

import scala.util.control.NonFatal
import scala.collection.immutable.ListMap
import org.apache.hadoop.hive.ql.parse._

import scalaz._
import Scalaz._
import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.annotation.compileTimeOnly
import scala.reflect.macros._
import au.com.cba.omnia.omnitool.{Result, ResultantMonad, ResultantOps, ToResultantMonadOps}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState

import scala.collection.convert.decorateAsScala._
import scala.concurrent._
import duration._
import scala.reflect.ClassTag
import scala.util.Random



/** Provides helpers for parsing/manipulating Hive queries */
object HiveQuery {
  import scala.collection.convert.decorateAsJava._

  def parseSelect(query: String): String \/ ASTNode = {
    try {
      val pd = new ParseDriver()
      \/.right(pd.parseSelect(query, null))
    } catch {
      case ex: Exception => \/.left(s"Error parsing the sql query: ${ex.getMessage}")
    }
  }

  def compileQuery(hiveConf: HiveConf, sources: Map[String, StructType], query: String): Throwable \/ Schema = HiveSupport.useHiveClassloader {
    SessionState.start(hiveConf)
    SessionState.get().setIsSilent(true)
    val dbName = s"test_${new Date().getTime}"
    val driver = new Driver(hiveConf)
    try {
      // Create the compilation environment
      createCompilationEnvironment(dbName, hiveConf, sources)

      // Initialise the variable substitution
      val variables =
        sources
          .keys
          .map(tableName => tableName -> s"${dbName}.${tableName}")
          .toMap
          .asJava

      SessionState.get().setHiveVariables(variables)

      // Run the query.
      driver.init()
      driver.compile(query)
      \/.right(driver.getSchema())
    } catch {
      case NonFatal(ex) => \/.left(new Exception(s"Error trying to run query '$query'", ex))
    } finally {
      driver.destroy()
    }
  }

  sealed abstract class HiveType {
    def allTypes: Set[HiveType]
    def hiveType: String
  }
  case class PrimitiveType[T](implicit manifest: ClassTag[T]) extends HiveType {
    def allTypes: Set[HiveType] = Set(this)
    def thriftTypeName = {
      val StringTag = implicitly[ClassTag[String]]
      manifest match {
        case ClassTag.Int     => "I32"
        case ClassTag.Long    => "I64"
        case ClassTag.Double  => "DOUBLE"
        case StringTag        => "STRING"
      }
    }
    def scalaType(c: whitebox.Context) = {
      import c.universe._
      val StringTag = implicitly[ClassTag[String]]
      manifest match {
        case ClassTag.Int     => tq"Int"
        case ClassTag.Long    => tq"Long"
        case ClassTag.Double  => tq"Double"
        case StringTag        => tq"String"
      }
    }
    def scalaTypeName = {
      val StringTag = implicitly[ClassTag[String]]
      manifest match {
        case ClassTag.Int     => "Integer"
        case ClassTag.Long    => "Long"
        case ClassTag.Double  => "Double"
        case StringTag        => "String"
      }
    }
    def hiveType = {
      val StringTag = implicitly[ClassTag[String]]
      manifest match {
        case ClassTag.Int     => "int"
        case ClassTag.Long    => "bigint"
        case ClassTag.Double  => "double"
        case StringTag        => "string"
      }
    }
    def placeholderValue(c: whitebox.Context) = {
      import c.universe._
      val StringTag = implicitly[ClassTag[String]]
      manifest match {
        case ClassTag.Int     => q"0"
        case ClassTag.Long    => q"0L"
        case ClassTag.Double  => q"0.0"
        case StringTag        => q"null"
      }
    }
  }
  case class MapType(key: HiveType, value: HiveType) extends HiveType {
    def allTypes: Set[HiveType] = (key.allTypes ++ value.allTypes) + this
    def hiveType = s"map<${key.hiveType},${value.hiveType}>"
  }

  def camelCaseField(str: String): String = {
    /** verbatim copy of scrooge 3.17::com.twitter.scrooge.ThriftStructMetaData.scala */
    str.takeWhile(_ == '_') +
      str
        .split('_')
        .filterNot(_.isEmpty)
        .zipWithIndex.map { case (part, ind) =>
        val first = if (ind == 0) part(0).toLower else part(0).toUpper
        val isAllUpperCase = part.forall(_.isUpper)
        val rest = if (isAllUpperCase) part.drop(1).toLowerCase else part.drop(1)
        new StringBuilder(part.size).append(first).append(rest)
      }
        .mkString
  }

  case class StructType(fields: ListMap[String, HiveType]) extends HiveType {
    def allTypes: Set[HiveType] = fields.values.flatMap(_.allTypes).toSet + this
    def hiveType = s"struct<${fields.toList.map({ case (name, hiveType) => s"${name}:${hiveType.hiveType}" }).mkString(",")}>"
    lazy val camelCasedFields: ListMap[String, HiveType] = {
      fields.map({ case (fieldName, value) =>
        camelCaseField(fieldName) -> value
      })
    }
  }
  case class ArrayType(valueType: HiveType) extends HiveType {
    def allTypes: Set[HiveType] = Set(this, valueType)
    def hiveType = s"array<${valueType.hiveType}>"
  }

  def parseHiveType(typeName: String): String \/ HiveType = {
    val MapExtractor    = "map<(.*),(.*)>".r
    val StructExtractor = "struct<(.*)>".r
    val FieldExtractor  = "(.*):(.*)".r
    val ArrayExtractor  = "array<(.*)>".r

    typeName match {
      case "int"    => \/.right(PrimitiveType[Int])
      case "string" => \/.right(PrimitiveType[String])
      case "double" => \/.right(PrimitiveType[Double])
      case "bigint" => \/.right(PrimitiveType[Long])
      // Map Types
      case  MapExtractor(keyType, valueType) =>
        for {
          key   <- parseHiveType(keyType)
          value <- parseHiveType(valueType)
        } yield MapType(key, value)
      // Struct Types
      case StructExtractor(fields) =>
        fields.split(",").toList.map({ case FieldExtractor(fieldName, typeName) =>
          parseHiveType(typeName).map(typ => fieldName -> typ)
        }).sequenceU.map(els => ListMap(els: _*)).map(StructType)
      // Array Types
      case ArrayExtractor(keyType) =>
        parseHiveType(keyType).map(ArrayType(_))
      // Unknown Types
      case unmatchedType => \/.left(unmatchedType)
    }
  }

  def createCompilationEnvironment(dbName: String, hiveConf: HiveConf, sources: Map[String, StructType]) = {
    import au.com.cba.omnia.beeswax._
    Hive.createDatabase(dbName).flatMap(_ => {
      Hive.getConfClient.map({ case (conf, client) =>
        sources.toList.map({ case (tableName, tableSchema) => {
          println(s"CREATING TABLE ${tableName} with SCHEMA ${tableSchema.hiveType}")
          val table = new Table()
          table.setDbName(dbName)
          table.setTableName(tableName)

          val sd = new StorageDescriptor()
          tableSchema.fields.toList.map({
            case (fieldName, fieldType) => {
              val fieldSchema = new FieldSchema(fieldName, fieldType.hiveType, "Table in compilation environment")
              sd.addToCols(fieldSchema)
            }
          })

          table.setSd(sd)

          ParquetFormat.applyFormat(table)

          try {
            client.createTable(table)
            Hive.value(true)
          } catch {
            case NonFatal(t) => Hive.error(s"Failed to create table $tableName in compilation environment (db = ${dbName})", t)
          }
        }})
      })
    }).run(hiveConf)
  }
}
