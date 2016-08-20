package com.rouesnel.typedsql.core

import scala.collection.immutable.ListMap

import scalaz._, Scalaz._

object HiveType {
  object parsers {
    import fastparse.all._

    def tinyIntParser  = P("tinyint").map(_ => TinyIntType)
    def smallIntParser = P("smallint").map(_ => ShortType)
    def intParser      = P("int").map(_ => IntType)
    def stringParser   = P("string").map(_ => StringType)
    def doubleParser   = P("double").map(_ => DoubleType)
    def bigintParser   = P("bigint").map(_ => LongType)
    def floatParser    = P("float").map(_ => FloatType)
    def booleanParser  = P("boolean").map(_ => BooleanType)
    def dateParser     = P("date").map(_ => DateType)
    def decimalParser  = P("decimal").map(_ => DecimalType())
    def decimalWithValuesParser =
      P("decimal(" ~ CharsWhile(_.isDigit).! ~ "," ~ CharsWhile(_.isDigit).! ~ ")").map({
        case (precision, scale) => DecimalType(precision.toInt, scale.toInt)
      })

    def scalarTypes = P(
      tinyIntParser |
        smallIntParser |
        intParser |
        stringParser |
        doubleParser |
        bigintParser |
        floatParser |
        booleanParser |
        dateParser |
        decimalParser |
        decimalWithValuesParser
    )

    def fieldIdentifier = P(CharsWhile(c => c.isLetterOrDigit || c == '_').!)

    def mapParser =
      P("map<" ~ hiveType ~ "," ~ hiveType ~ ">").map({
        case (key, value) => MapType(key, value)
      })
    def arrayParser = P("array<" ~ hiveType ~ ">").map(ArrayType(_))

    def fieldParser: Parser[(String, HiveType)] =
      P(fieldIdentifier ~ ":" ~ hiveType)
    def structParser =
      P("struct<" ~ fieldParser.rep(sep = ",") ~ ">").map(ls => StructType(ls.toList))

    def hiveType: Parser[HiveType] =
      scalarTypes | mapParser | arrayParser | structParser
  }

  /** Parses the HiveType object from a Hive textual description */
  def parseHiveType(typeName: String): String \/ HiveType = {
    \/.fromTryCatchNonFatal({
        parsers.hiveType.parse(typeName).get.value
      })
      .leftMap(_.toString)
  }

  def camelCaseFieldName(str: String): String = {

    /** verbatim copy of scrooge 3.17::com.twitter.scrooge.ThriftStructMetaData.scala */
    str.takeWhile(_ == '_') +
      str
        .split('_')
        .filterNot(_.isEmpty)
        .zipWithIndex
        .map {
          case (part, ind) =>
            val first          = if (ind == 0) part(0).toLower else part(0).toUpper
            val isAllUpperCase = part.forall(_.isUpper)
            val rest =
              if (isAllUpperCase) part.drop(1).toLowerCase else part.drop(1)
            new StringBuilder(part.size).append(first).append(rest)
        }
        .mkString
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

final case object TinyIntType extends PrimitiveType {
  def hiveType       = "tinyint"
  def scalaTypeName  = "Byte"
  def thriftTypeName = "BYTE"
}
final case object ShortType extends PrimitiveType {
  def hiveType       = "smallint"
  def scalaTypeName  = "Short"
  def thriftTypeName = "I16"
}
final case object IntType extends PrimitiveType {
  def hiveType       = "int"
  def scalaTypeName  = "Integer"
  def thriftTypeName = "I32"
}
final case object LongType extends PrimitiveType {
  def hiveType       = "bigint"
  def scalaTypeName  = "Long"
  def thriftTypeName = "I64"
}
final case object FloatType extends PrimitiveType {
  def hiveType       = "float"
  def scalaTypeName  = "Float"
  def thriftTypeName = "DOUBLE"
}
final case object DoubleType extends PrimitiveType {
  def hiveType       = "double"
  def scalaTypeName  = "Double"
  def thriftTypeName = "DOUBLE"
}
final case class DecimalType(precision: Int = 10, scale: Int = 0) extends PrimitiveType {
  def hiveType       = s"decimal(${precision},${scale})"
  def scalaTypeName  = "scala.math.BigDecimal"
  def thriftTypeName = "STRING"
}
final case object StringType extends PrimitiveType {
  def hiveType       = "string"
  def scalaTypeName  = "String"
  def thriftTypeName = "STRING"
}
final case object BooleanType extends PrimitiveType {
  def hiveType       = "boolean"
  def scalaTypeName  = "Boolean"
  def thriftTypeName = "BOOL"
}
final case object DateType extends PrimitiveType {
  def hiveType       = "date"
  def scalaTypeName  = "java.util.Date"
  def thriftTypeName = "I32"
}
final case class MapType(key: HiveType, value: HiveType) extends HiveType {
  def allTypes: Set[HiveType] = (key.allTypes ++ value.allTypes) + this
  def hiveType                = s"map<${key.hiveType},${value.hiveType}>"
}

object StructType {
  def apply(fields: ListMap[String, HiveType]): StructType =
    StructType(fields.toList)
}
final case class StructType(fieldList: List[(String, HiveType)]) extends HiveType {
  @transient lazy val fields  = ListMap(fieldList: _*)
  def allTypes: Set[HiveType] = fields.values.flatMap(_.allTypes).toSet + this
  def hiveType =
    s"struct<${fields.toList.map({ case (name, hiveType) => s"${name}:${hiveType.hiveType}" }).mkString(",")}>"

  /** Fields/types with the field names camelcased */
  @transient lazy val camelCasedFields: ListMap[String, HiveType] = {
    fields.map({
      case (fieldName, value) =>
        HiveType.camelCaseFieldName(fieldName) -> value
    })
  }
}
final case class ArrayType(valueType: HiveType) extends HiveType {
  def allTypes: Set[HiveType] = Set(this, valueType)
  def hiveType                = s"array<${valueType.hiveType}>"
}
