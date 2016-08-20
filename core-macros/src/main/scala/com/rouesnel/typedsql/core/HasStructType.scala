package com.rouesnel.typedsql.core


import com.twitter.scrooge.ThriftStruct

import scala.reflect.macros._

/** Type class that captures the Hive struct type associated with the type T */
case class HasStructType[T](structType: StructType)

/** This companion object will automatically materialise the HasStructType type class */
object HasStructType {
  implicit def materialiseHasStructType[T <: ThriftStruct]: HasStructType[T] = macro impl[T]

  def impl[T](c: Context)(implicit typ: c.WeakTypeTag[T]): c.Expr[HasStructType[T]] = {
    import c.universe._

    val hiveType = new ThriftHiveTypeMacro[c.type](c)
      .convertScalaToHiveType(c.weakTypeOf[T])

    val result =
      q"com.rouesnel.typedsql.core.HasStructType[${typ}](com.rouesnel.typedsql.core.HiveType.parseHiveType(${Literal(Constant(hiveType.hiveType))}).toOption.get.asInstanceOf[com.rouesnel.typedsql.core.StructType])"

    c.Expr[HasStructType[T]](result)
  }
}