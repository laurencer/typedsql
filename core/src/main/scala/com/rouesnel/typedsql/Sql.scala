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
    val sourceMapping = new SourceMapping(c)
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
          //HiveQuery.parseSelect(sqlStatement)
          // .fold(c.abort(c.enclosingPosition, _), identity)

          // Get sources and create temp tables mapping to them.
          val sources = sourceMapping.readSourcesField(stats)

          // Retrieve the schema from the compiled query.
          val schema = HiveQuery.compileQuery(hiveConf, sources, sqlStatement)
            .fold(ex => c.abort(c.enclosingPosition, ex.toString), identity)

          // Parse the types from the schema.
          val outputRecordFields = schema.getFieldSchemas.asScala.map(fieldSchema => {
            val fieldName = fieldSchema.getName
            val fieldType =
              HiveType.parseHiveType(fieldSchema.getType)
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
            .mapValues(fields => StructType(ListMap(fields: _*)))

          // Find all the structs to generate appropriate types.
          val structs = outputRecordFields
            .flatMap({ case (_, hiveType) => hiveType.allTypes })
            .toList
            .collect({ case struct: StructType => struct})

          // Map the nested structs to the generated ones and remove any nested fields
          val fieldsToGenerate = outputRecordFields
            .filter({ case (fieldName, _) => !fieldName.contains(".") }) ++
            implicitStructs.toList.map({ case (fieldNameParts, fieldStruct) =>
              fieldNameParts.mkString(".") -> fieldStruct
            })

          // Make the returned row itself a struct for simplicity/elegance.
          val outputRecord = StructType(ListMap(fieldsToGenerate: _*))

          // Compose all of the structs together.
          val allStructs = (outputRecord +: (implicitStructs.values.toList ++ structs))
            .zipWithIndex
            .toMap

          // Resolve the type of each Hive Type to a concrete Scala Type
          def structName(idx: Int): TypeName =
            if (idx == 0) TypeName(s"Row") // Special case - first struct is the actual output record.
            else TypeName(s"Struct${idx}")


          def resolveType(fieldType: HiveType): Tree = fieldType match {
            case p: PrimitiveType[_]          => p.scalaType(c)
            case ArrayType(valueType)         => tq"List[${resolveType(valueType)}]"
            case MapType(keyType, valueType)  => tq"Map[${resolveType(keyType)}, ${resolveType(valueType)}]"
            case s: StructType                => tq"${structName(allStructs(s))}"
          }

          def hiveTypeToThriftTypeName(typ: HiveType): TermName = typ match {
            case p: PrimitiveType[_] => TermName(p.thriftTypeName)
            case m: MapType          => TermName("MAP")
            case l: ArrayType        => TermName("LIST")
            case s: StructType       => TermName("STRUCT")
          }

          def hiveTypeToReadMethod(typ: HiveType): Tree = typ match {
            case p: PrimitiveType[_] => q"_iprot.${TermName("read" + p.thriftTypeName.toLowerCase.capitalize)}"
            case s: StructType       => q"${structName(allStructs(s)).toTermName}.decode(_iprot)"
          }

          def buildMapDecode(keyType: Tree, valueType: Tree, readKey: Tree, readValue: Tree) = {
            q"""
               val _map = _iprot.readMapBegin()
               if (_map.size == 0) {
                 _iprot.readMapEnd()
                 Map.empty[${keyType}, ${valueType}]
               } else {
                 val _rv = new scala.collection.mutable.HashMap[${keyType}, ${valueType}]
                 var _i = 0
                 while (_i < _map.size) {
                   val _key = ${readKey}
                   val _value = ${readValue}
                   _rv(_key) = _value
                   _i += 1
                }
                _iprot.readMapEnd()
                _rv.toMap
               }
            """
          }

          def buildArrayDecode(valueType: Tree, readValue: Tree) = {
           q"""
               val _list = _iprot.readListBegin()
               if (_list.size == 0) {
                 _iprot.readListEnd()
                 Nil
               } else {
                 val _rv = new scala.collection.mutable.ArrayBuffer[${valueType}](_list.size)
                 var _i = 0
                 while (_i < _list.size) {
                   _rv += {
                       ${readValue}
                   }
                   _i += 1
                 }
                 _iprot.readListEnd()
                 _rv.toList
               }
            """
          }

          def buildThriftDecode(structIdx: Int, fields: List[(String, HiveType)]) = {
            def readMethodName(fieldName: String) = TermName("read" + fieldName.capitalize + "Value")

            def readerForType(hiveType: HiveType): Tree = hiveType match {
              case p: PrimitiveType[_] => hiveTypeToReadMethod(hiveType)
              case m: MapType          => buildMapDecode(resolveType(m.key), resolveType(m.value), readerForType(m.key), readerForType(m.value))
              case a: ArrayType        => buildArrayDecode(resolveType(a.valueType), readerForType(a.valueType))
              case s: StructType       => q"${structName(allStructs(s)).toTermName}.decode(_iprot)"
            }

            // Methods for reading each field.
            val readMethods = fields.map({ case (fieldName, hiveType) =>
              q"""private def ${readMethodName(fieldName)}(_iprot: org.apache.thrift.protocol.TProtocol): ${resolveType(hiveType)} = {
                    ${readerForType(hiveType)}
                  }
               """})

            // Mutable placeholder fields used to store each field as its read.
            val placeholderFields = fields.map({ case (fieldName, hiveType) => {
              val placeholderValue = hiveType match {
                case p: PrimitiveType[_] => p.placeholderValue(c)
                case _ => q"null"
              }
              q"var ${TermName(fieldName)}: ${resolveType(hiveType)} = ${placeholderValue}"
            }})

            // Extracts a given field into the mutable placeholder (or throws an error on invalid data)
            val fieldMatchers = fields.zipWithIndex.map({ case ((fieldName, hiveType), idx) =>
              val fieldExtractor =
                q"""
                   _field.`type` match {
                    case org.apache.thrift.protocol.TType.${hiveTypeToThriftTypeName(hiveType)} => {
                      ${TermName(fieldName)} = ${readMethodName(fieldName)}(_iprot)
                    }
                   case _actualType => {
                     val _expectedType = org.apache.thrift.protocol.TType.${hiveTypeToThriftTypeName(hiveType)}
                     val errorMessage = ("Received wrong type for field " + ${Literal(Constant(fieldName))} + " (expected=%s, actual=%s).").format(
                                           ttypeToHuman(_expectedType),
                                           ttypeToHuman(_actualType)
                                         )
                     println("GOT ERROR " + errorMessage)
                     throw new org.apache.thrift.protocol.TProtocolException(errorMessage)
                     }
                   }
                 """
              cq"""${Literal(Constant(idx + 1))} => { $fieldExtractor }"""
            })

            // Builds the class from the temporary/placeholder fields.
            def classConstructor = {
              val fieldValues = fields.map({ case (fieldName, hiveType) => {
                q"${TermName(fieldName)}"
              }})
              q"${structName(structIdx).toTermName}(..$fieldValues)"
            }

            // Build the actual decode method.
            val decode = q"""
              override def decode(_iprot: org.apache.thrift.protocol.TProtocol): ${structName(structIdx)} = {
                ..${placeholderFields}
                var _done = false
                _iprot.readStructBegin()
                while(!_done) {
                  val _field = _iprot.readFieldBegin()
                  if (_field.`type` == org.apache.thrift.protocol.TType.STOP) {
                    _done = true
                  } else {
                    _field.id match {
                      case ..$fieldMatchers
                    }
                    _iprot.readFieldEnd()
                  }
                }
                _iprot.readStructEnd()
                ${classConstructor}
              }
             """

            readMethods :+ decode
          }

          // Generate structs
          val generatedStructs = allStructs.map({ case (struct, idx) => {
            val fields = struct.fields.toList.map({ case (fieldName, fieldType) =>
              q"${TermName(HiveType.camelCaseFieldName(fieldName))}: ${resolveType(fieldType)}"
            })
            val thriftFields = struct.fields.toList.zipWithIndex.flatMap({ case ((fieldName, fieldType), idx) =>
              List(q"""
                val ${TermName(HiveType.camelCaseFieldName(fieldName).capitalize + "Field")} = new org.apache.thrift.protocol.TField(${Literal(Constant(fieldName))}, org.apache.thrift.protocol.TType.${hiveTypeToThriftTypeName(fieldType)}, ${Literal(Constant(idx + 1))})""",
                q"""val ${TermName(HiveType.camelCaseFieldName(fieldName).capitalize + "FieldManifest")} = implicitly[Manifest[${resolveType(fieldType)}]]""")
            })

            // Include the ttypeToHuman method (needed for constructing decoders)
            val ttypeToHumanMethod = q"""
             private def ttypeToHuman(byte: Byte) = {
              // from https://github.com/apache/thrift/blob/master/lib/java/src/org/apache/thrift/protocol/TType.java
                import org.apache.thrift.protocol.TType
                byte match {
                  case TType.STOP   => "STOP"
                  case TType.VOID   => "VOID"
                  case TType.BOOL   => "BOOL"
                  case TType.BYTE   => "BYTE"
                  case TType.DOUBLE => "DOUBLE"
                  case TType.I16    => "I16"
                  case TType.I32    => "I32"
                  case TType.I64    => "I64"
                  case TType.STRING => "STRING"
                  case TType.STRUCT => "STRUCT"
                  case TType.MAP    => "MAP"
                  case TType.SET    => "SET"
                  case TType.LIST   => "LIST"
                  case TType.ENUM   => "ENUM"
                  case _            => "UNKNOWN"
                }
              }
            """

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
                   ..${ttypeToHumanMethod}
                   override def encode(t: ${structName(idx)}, oprot: org.apache.thrift.protocol.TProtocol): Unit = {
                      println("Encoding is not supported for generated Hive tables.");
                      ???
                   }
                   ..${buildThriftDecode(idx, struct.camelCasedFields.toList)}
                 }
                """
            )
          }}).toList.flatten

          val amendedParents = parents :+ tq"CompiledSqlQuery"
          q"""$mods object $tpname extends ..$amendedParents {
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
