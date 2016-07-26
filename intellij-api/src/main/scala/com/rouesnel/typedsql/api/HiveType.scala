package com.rouesnel.typedsql.api

import scala.collection.immutable.ListMap

/** ADT of (nearly) all the possible types that Hive supports */
sealed abstract class HiveType {
}

/** Primitive scalar types */
case class PrimitiveType[T](name: String) extends HiveType {
}

case class MapType(key: HiveType, value: HiveType) extends HiveType {
}

case class StructType(fields: ListMap[String, HiveType]) extends HiveType {
}
case class ArrayType(valueType: HiveType) extends HiveType {
}

