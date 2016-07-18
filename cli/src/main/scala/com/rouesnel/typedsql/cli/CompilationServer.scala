package com.rouesnel.typedsql.cli

import akka.actor._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.convert.decorateAsScala._
import com.rouesnel.typedsql._
import com.rouesnel.typedsql.api._
import org.apache.hadoop.hive.metastore.api.Schema

import scala.collection.immutable.ListMap

import scalaz._

object CompilationServer extends App {
  implicit val system = ActorSystem("typedsql-cli")
  val listener = system.actorOf(Props[Listener], name = "listener")
}

class Listener extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  val conf = Await.result(HiveSupport.initialize(true, println(_)), 15.seconds)

  val cache = scala.collection.mutable.WeakHashMap[CompilationRequest, CompilationResponse]()

  def receive = {
    case cr: CompilationRequest => {
      val result = \/.fromTryCatchNonFatal(cache.getOrElseUpdate(cr, {
        val rawSqlText = cr.query.trim.replaceAll("\\n", " ")
        /** IntelliJ includes the surrounding quotes. This removes them. */
        val sqlText = rawSqlText.dropWhile(_ == '"').reverse.dropWhile(_ == '"').reverse
        HiveQuery.compileQuery(conf, sqlText).flatMap(Converter.produceCaseClass(_, "Row")).fold(
          error  => throw error,
          generatedCaseClass => CompilationResponse(generatedCaseClass)
        )
      }))

      result.fold(
        error    => println(s"Error: $error"),
        response => sender ! response
      )
    }
    case other: String => {
      println(other)
    }
  }
}

object Converter {
  def hiveTypeToScalaType(hiveType: HiveQuery.HiveType): String = hiveType match {
    case p: HiveQuery.PrimitiveType[_] => p.scalaTypeName
    case m: HiveQuery.MapType          => s"scala.collection.Map[${hiveTypeToScalaType(m.key)}, ${hiveTypeToScalaType(m.value)}]"
    case l: HiveQuery.ArrayType        => s"scala.collection.List[${hiveTypeToScalaType(l.valueType)}]"
    case s: HiveQuery.StructType       => {
      val structuralTypeFields = s.camelCasedFields.toList.map({ case (fieldName, hiveType) => {
        s"def ${fieldName}: ${hiveTypeToScalaType(hiveType)}"
      }}).mkString("; ")
      s"{ ${structuralTypeFields} }"
    }
  }

  def produceCaseClass(schema: Schema, className: String = "Row"): Throwable \/ String = \/.fromTryCatchNonFatal {
    // Get all of the fields in the row schema.
    val outputRecordFields = schema.getFieldSchemas.asScala.map(fieldSchema => {
      val fieldName = fieldSchema.getName
      val fieldType =
        HiveQuery.parseHiveType(fieldSchema.getType)
          .fold(missingType => throw new Exception(s"Missing type: $missingType"), identity)
      (fieldName, fieldType)
    })

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
      .mapValues(fields => HiveQuery.StructType(ListMap(fields: _*)))

    // Map the nested structs to the generated ones and remove any nested fields
    val fieldsToGenerate = HiveQuery.StructType(ListMap(outputRecordFields
      .filter({ case (fieldName, _) => !fieldName.contains(".") }) ++
      implicitStructs.toList.map({ case (fieldNameParts, fieldStruct) =>
        fieldNameParts.mkString(".") -> fieldStruct
      }): _*))

    val caseClassFields = fieldsToGenerate.camelCasedFields.toList.map({ case (fieldName, fieldType) => {
      s"${fieldName}: ${hiveTypeToScalaType(fieldType)}"
    }}).mkString(", ")

    s"case class ${className}(${caseClassFields})"
  }
}