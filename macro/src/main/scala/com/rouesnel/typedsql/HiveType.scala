package com.rouesnel.typedsql

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

/** Primitive scalar types */
case class PrimitiveType[T](implicit manifest: ClassTag[T]) extends HiveType {
  def allTypes: Set[HiveType] = Set(this)
  /** Name of the Thrift TTYPE */
  def thriftTypeName = {
    val StringTag = implicitly[ClassTag[String]]
    manifest match {
      case ClassTag.Int     => "I32"
      case ClassTag.Long    => "I64"
      case ClassTag.Double  => "DOUBLE"
      case StringTag        => "STRING"
    }
  }

  /** Scala compiler type representation */
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

  /** Scala textual name for the type */
  def scalaTypeName = {
    val StringTag = implicitly[ClassTag[String]]
    manifest match {
      case ClassTag.Int     => "Integer"
      case ClassTag.Long    => "Long"
      case ClassTag.Double  => "Double"
      case StringTag        => "String"
    }
  }

  /** Hive's textual representation of the field */
  def hiveType = {
    val StringTag = implicitly[ClassTag[String]]
    manifest match {
      case ClassTag.Int     => "int"
      case ClassTag.Long    => "bigint"
      case ClassTag.Double  => "double"
      case StringTag        => "string"
    }
  }

  /** Used where default values are needed */
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

