package com.rouesnel.typedsql.core

import com.twitter.scrooge.ThriftStruct

import scala.reflect.macros.Context

class ScalaHiveTypeMapping[C <: Context](val c: C) {
  // List of all the Scala types that map to Hive typess
  val seqType        = c.universe.weakTypeOf[Seq[_]]
  val mapType        = c.universe.weakTypeOf[scala.collection.Map[_, _]]
  val intType        = c.universe.weakTypeOf[Int]
  val longType       = c.universe.weakTypeOf[scala.Long]
  val javaLongType   = c.universe.weakTypeOf[java.lang.Long]
  val booleanType    = c.universe.weakTypeOf[Boolean]
  val doubleType     = c.universe.weakTypeOf[Double]
  val stringType     = c.universe.weakTypeOf[String]
  val dateType       = c.universe.weakTypeOf[java.sql.Date]
  val shortType      = c.universe.weakTypeOf[Short]
  val byteType       = c.universe.weakTypeOf[Byte]
  val floatType      = c.universe.weakTypeOf[Float]
  val bigDecimalType = c.universe.weakTypeOf[java.math.BigDecimal]
  val thriftType     = c.universe.weakTypeOf[ThriftStruct]
}
