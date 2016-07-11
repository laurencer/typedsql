package com.rouesnel.typedsql

import java.io.File

import scala.util.control.NonFatal
import org.apache.hadoop.hive.ql.parse._

import scalaz._
import Scalaz._
import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.annotation.compileTimeOnly
import scala.reflect.macros._
import au.com.cba.omnia.omnitool.{Result, ResultantMonad, ResultantOps, ToResultantMonadOps}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState

import scala.collection.convert.decorateAsScala._
import scala.concurrent._
import duration._
import scala.reflect.ClassTag

@compileTimeOnly("enable macro paradise to expand macro annotations")
class SqlQuery extends StaticAnnotation {
  def macroTransform(annottees: Any*) = macro SqlQuery.impl
}

object SqlQuery {
  import au.com.cba.omnia.beeswax.Hive
  import org.apache.hadoop.hive.ql.Driver
  import org.apache.hadoop.hive.ql.session.SessionState

  val futureHiveConf = HiveSupport.initialize()
  def hiveConf = Await.result(futureHiveConf, 25.seconds)

  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val result = {
      annottees.map(_.tree).toList match {
        case q"$mods object $tpname extends ..$parents { ..$stats }" :: Nil => {
          val sqlLiteral = stats
            .collect({ case q"def query = $sqlQuery" => sqlQuery })
            .headOption
            .getOrElse(c.abort(c.enclosingPosition, "Must have a function called `query`."))
            match {
              case Literal(Constant(code)) => code
              case _ => c.abort(c.enclosingPosition, "Query function must be a literal string.")
          }

          val sqlStatement = sqlLiteral.toString.trim.replaceAll("\\n", " ")
          if (sqlStatement.split(";").length != 1) {
            c.abort(c.enclosingPosition, s"Only a single SQL statement is supported. Please remove any ';'s from $sqlLiteral")
          }

          // Try to parse and exit here first.
          // This is quicker than compiling and ensures only a SELECT statement is used
          // rather than any DDL statements.
          HiveQuery.parseSelect(sqlStatement)
            .fold(c.abort(c.enclosingPosition, _), identity)

          // Retrieve the schema from the compiled query.
          val schema = HiveQuery.compileQuery(hiveConf, sqlStatement)
            .fold(ex => c.abort(c.enclosingPosition, ex.toString), identity)

          // Parse the types from the schema.
          val outputRecordFields = schema.getFieldSchemas.asScala.map(fieldSchema => {
            val fieldName = fieldSchema.getName
            val fieldType =
              HiveQuery.parseHiveType(fieldSchema.getType)
                  .fold(missingType => c.abort(c.enclosingPosition, s"Could not find Scala type to match Hive type ${missingType} (in ${fieldSchema.getType}) for column ${fieldName}"), identity)
            (fieldName, fieldType)
          }).toList

          // Map all nested fields on the top-level schema to a struct
          // E.g. SELECT * FROM test1 INNER JOIN test2
          // will return test1.foo, test1.bar, test2.baz, test2.bax
          //
          // test1 and test2 should form reusable structs.
          val implicitStructs = outputRecordFields
              .collect({ case (fieldName, fieldType) if fieldName.contains(".") => {
                val fieldParts = fieldName.split("\\.").toList
                (fieldParts.dropRight(1), (fieldParts.last, fieldType))
              }})
            .groupBy({ case (parent, (fieldName, fieldType)) => parent })
            .mapValues(_.map({ case (parent, fieldInfo) => fieldInfo }))
            .mapValues(fields => HiveQuery.StructType(fields.toMap))

          // Find all the structs to generate appropriate types.
          val structs = outputRecordFields
            .flatMap({ case (_, hiveType) => hiveType.allTypes })
            .toList
            .collect({ case struct: HiveQuery.StructType => struct})

          // Map the nested structs to the generated ones and remove any nested fields
          val fieldsToGenerate = outputRecordFields
            .filter({ case (fieldName, _) => !fieldName.contains(".") }) ++
            implicitStructs.toList.map({ case (fieldNameParts, fieldStruct) =>
              fieldNameParts.mkString(".") -> fieldStruct
            })

          // Make the returned row itself a struct for simplicity/elegance.
          val outputRecord = HiveQuery.StructType(fieldsToGenerate.toMap)


          // Compose all of the structs together.
          val allStructs = (outputRecord +: (implicitStructs.values.toList ++ structs))
            .zipWithIndex
            .toMap

          // Resolve the type of each Hive Type to a concrete Scala Type
          def structName(idx: Int): TypeName =
            if (idx == 0) TypeName(s"OutputRecord") // Special case - first struct is the actual output record.s
            else TypeName(s"Struct${idx}")


          def resolveType(fieldType: HiveQuery.HiveType): Tree = fieldType match {
            case p: HiveQuery.PrimitiveType[_]          => p.scalaType(c)
            case HiveQuery.ArrayType(valueType)         => tq"List[${resolveType(valueType)}]"
            case HiveQuery.MapType(keyType, valueType)  => tq"Map[${resolveType(keyType)}, ${resolveType(valueType)}]"
            case s: HiveQuery.StructType                => tq"${structName(allStructs(s))}"
          }

          def hiveTypeToThriftTypeName(typ: HiveQuery.HiveType): TermName = typ match {
            case p: HiveQuery.PrimitiveType[_] => TermName(p.thriftTypeName)
            case m: HiveQuery.MapType          => TermName("MAP")
            case l: HiveQuery.ArrayType        => TermName("LIST")
            case s: HiveQuery.StructType       => TermName("STRUCT")
          }

          // Generate structs
          val generatedStructs = allStructs.map({ case (struct, idx) => {
            val fields = struct.fields.toList.map({ case (fieldName, fieldType) =>
              q"${TermName(fieldName)}: ${resolveType(fieldType)}"
            })
            val thriftFields = struct.fields.toList.zipWithIndex.map({ case ((fieldName, fieldType), idx) =>
              q"""
                val ${TermName(fieldName + "Field")} = new org.apache.thrift.protocol.TField(${Literal(Constant(fieldName))}, org.apache.thrift.protocol.TType.${hiveTypeToThriftTypeName(fieldType)}, ${Literal(Constant(idx + 1))})
                val ${TermName(fieldName + "FieldManifest")} = implicitly[Manifest[${resolveType(fieldType)}]]
               """
            })
            // Generate the case class for the struct as well as the ThriftStructCodec3 for Parquet
            // compatibility
            List(
              q"""
                case class ${structName(idx)}(..$fields) extends com.twitter.scrooge.ThriftStruct {
                  override def write(oprot: org.apache.thrift.protocol.TProtocol): Unit = { println("WRITE"); ??? }
                }
               """,
              q"""
                 object ${structName(idx).toTermName} extends com.twitter.scrooge.ThriftStructCodec3[${structName(idx)}] {
                   ..${thriftFields}
                   override def encode(t: ${structName(idx)}, oprot: org.apache.thrift.protocol.TProtocol): Unit = { println("ENCODE"); ??? }
                   override def decode(iprot: org.apache.thrift.protocol.TProtocol): ${structName(idx)} = { println("DECODE"); ??? }
                 }
                """
            )
          }}).toList.flatten

          val generatedOutputRecordFields = fieldsToGenerate.map({ case (fieldName, fieldType) =>
            q"""def ${TermName(fieldName)}: ${resolveType(fieldType)}"""
          })

          q"""$mods object $tpname extends ..$parents {
            ..$stats

            ..${generatedStructs}
          }"""

        }
        case _ => c.abort(c.enclosingPosition, "Annotation @SqlQuery can only apply to objects")
      }
    }
    println(result)
    c.Expr[Any](result)
  }
}

/** Provides helpers for parsing/manipulating Hive queries */
object HiveQuery {
  def parseSelect(query: String): String \/ ASTNode = {
    try {
      val pd = new ParseDriver()
      \/.right(pd.parseSelect(query, null))
    } catch {
      case ex: Exception => \/.left(s"Error parsing the sql query: ${ex.getMessage}")
    }
  }

  def compileQuery(hiveConf: HiveConf, query: String): Throwable \/ Schema = HiveSupport.useHiveClassloader {
    SessionState.start(hiveConf)
    SessionState.get().setIsSilent(true)
    val driver = new Driver(hiveConf)
    try {
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
  }
  case class PrimitiveType[T](implicit manifest: ClassTag[T]) extends HiveType {
    def allTypes: Set[HiveType] = Set(this)
    def thriftTypeName = {
      val StringTag = implicitly[ClassTag[String]]
      manifest match {
        case ClassTag.Int     => "I32"
        case ClassTag.Double  => "DOUBLE"
        case StringTag        => "STRING"
      }
    }

    def scalaType(c: whitebox.Context) = {
      import c.universe._
      val StringTag = implicitly[ClassTag[String]]
      manifest match {
        case ClassTag.Int     => tq"Int"
        case ClassTag.Double  => tq"Double"
        case StringTag        => tq"String"
      }
    }
  }
  case class MapType(key: HiveType, value: HiveType) extends HiveType {
    def allTypes: Set[HiveType] = (key.allTypes ++ value.allTypes) + this
  }
  case class StructType(fields: Map[String, HiveType]) extends HiveType {
    def allTypes: Set[HiveType] = fields.values.flatMap(_.allTypes).toSet + this
  }
  case class ArrayType(valueType: HiveType) extends HiveType {
    def allTypes: Set[HiveType] = Set(this, valueType)
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
        }).sequenceU.map(_.toMap).map(StructType)
      // Array Types
      case ArrayExtractor(keyType) =>
        parseHiveType(keyType).map(ArrayType(_))
      // Unknown Types
      case unmatchedType => \/.left(unmatchedType)
    }
  }
}

/** Provides functions to support a fake Hive environment */
object HiveSupport {
  import scala.concurrent.ExecutionContext.Implicits.global

  def useHiveClassloader[T](f: => T): T = {
    // Replace the classloader with the correct path.
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(classOf[HiveConf].getClassLoader)
    // Run the computation
    val result = \/.fromTryCatchNonFatal(f)
    // Reset the contextClassLoader.
    Thread.currentThread().setContextClassLoader(contextClassLoader)
    // Return the result
    result match {
      case \/-(value) => value
      case -\/(error) => throw error
    }
  }

  def initialize(): Future[HiveConf] = Future { useHiveClassloader {
    import org.apache.hadoop.hive.conf.HiveConf, HiveConf.ConfVars, ConfVars._
    import org.apache.hadoop.hive.metastore._, api._

    val tempDir = File.createTempFile("hive", "compile")
    tempDir.delete()
    tempDir.mkdir()

    /* Creates a fake local instance for Hive - stolen from Thermometer Hive */
    lazy val hiveDir: String       = tempDir.toString
    lazy val hiveDb: String        = s"$hiveDir/hive_db"
    lazy val hiveWarehouse: String = s"$hiveDir/warehouse"
    lazy val derbyHome: String     = s"$hiveDir/derby"
    lazy val hiveConf: HiveConf    = new HiveConf <| (conf => {
      conf.setVar(METASTOREWAREHOUSE, hiveWarehouse)
    })

    // Export the warehouse path so it gets picked up when a new hive conf is instantiated somehwere else.
    System.setProperty(METASTOREWAREHOUSE.varname, hiveWarehouse)
    System.setProperty("derby.system.home", derbyHome)
    // Export the derby db file location so it is different for each test.
    System.setProperty("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$hiveDb;create=true")
    System.setProperty("hive.metastore.ds.retry.attempts", "0")

    // Wait for the Hive client to initialise
    val client = RetryingMetaStoreClient.getProxy(
      hiveConf,
      new HiveMetaHookLoader() {
        override def getHook(tbl: Table) = null
      },
      classOf[HiveMetaStoreClient].getName()
    )
    try {
      client.getAllDatabases()
    } finally {
      client.close
    }

    import au.com.cba.omnia.beeswax.Hive
    val successfullyCreated = Hive.createParquetTable[Person]("test_db", "test", Nil)
      .run(hiveConf)

    println(s"Created test table: $successfullyCreated")

    hiveConf
  }}
}