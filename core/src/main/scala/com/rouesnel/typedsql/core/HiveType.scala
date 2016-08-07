package com.rouesnel.typedsql.core

import scala.collection.immutable.ListMap
import scala.reflect._, macros.whitebox

import scalaz._, Scalaz._

object HiveType {
  def parseHiveType(typeName: String): String \/ HiveType = {
    val MapExtractor    = "map<(.*),(.*)>".r
    val StructExtractor = "struct<(.*)>".r
    val FieldExtractor  = "(.*):(.*)".r
    val ArrayExtractor  = "array<(.*)>".r

    typeName match {
      case "int"    => \/.right(IntType)
      case "string" => \/.right(StringType)
      case "double" => \/.right(DoubleType)
      case "bigint" => \/.right(LongType)
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

  def camelCaseFieldName(str: String): String = {
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
      }.mkString
  }
}

/** ADT of (nearly) all the possible types that Hive supports */
sealed abstract class HiveType {
  /** Generates a set of all Hive types in the hierarchy (e.g. for structs returns all nested types) */
  def allTypes: Set[HiveType]

  /** The textual representation Hive uses for the type (i.e. that can be used in CREATE TABLE syntax) */
  def hiveType: String
}

sealed abstract class PrimitiveType extends HiveType {
  def allTypes: Set[HiveType] = Set(this)
  def scalaTypeName: String
  def thriftTypeName: String
}

case object ShortType extends PrimitiveType {
  def hiveType = "int"
  def scalaTypeName = "Short"
  def thriftTypeName = "I16"
}
case object IntType extends PrimitiveType {
  def hiveType = "int"
  def scalaTypeName = "Integer"
  def thriftTypeName = "I32"
}
case object LongType extends PrimitiveType {
  def hiveType = "bigint"
  def scalaTypeName = "Long"
  def thriftTypeName = "I64"
}
case object DoubleType extends PrimitiveType {
  def hiveType = "double"
  def scalaTypeName = "Double"
  def thriftTypeName = "DOUBLE"
}
case object StringType extends PrimitiveType {
  def hiveType = "string"
  def scalaTypeName = "String"
  def thriftTypeName = "STRING"
}

case class MapType(key: HiveType, value: HiveType) extends HiveType {
  def allTypes: Set[HiveType] = (key.allTypes ++ value.allTypes) + this
  def hiveType = s"map<${key.hiveType},${value.hiveType}>"
}

case class StructType(fields: ListMap[String, HiveType]) extends HiveType {
  def allTypes: Set[HiveType] = fields.values.flatMap(_.allTypes).toSet + this
  def hiveType = s"struct<${fields.toList.map({ case (name, hiveType) => s"${name}:${hiveType.hiveType}" }).mkString(",")}>"

  /** Fields/types with the field names camelcased */
  lazy val camelCasedFields: ListMap[String, HiveType] = {
    fields.map({ case (fieldName, value) =>
      HiveType.camelCaseFieldName(fieldName) -> value
    })
  }
}
case class ArrayType(valueType: HiveType) extends HiveType {
  def allTypes: Set[HiveType] = Set(this, valueType)
  def hiveType = s"array<${valueType.hiveType}>"
}

