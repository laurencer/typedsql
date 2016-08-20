package com.rouesnel.typedsql.core

import com.twitter.scrooge.ThriftStruct

import scala.reflect.macros._

/** From a Scala Type - produces a Hive StructType */
object GenerateHiveStruct {
  def apply[T <: ThriftStruct] = macro impl[T]

  def impl[T](c: Context)(implicit typ: c.WeakTypeTag[T]): c.Expr[StructType] = {
    import c.universe._

    val hiveType = new ThriftHiveTypeMacro[c.type](c).convertScalaToHiveType(c.weakTypeOf[T])

    c.Expr[StructType](
      q"com.rouesnel.typedsql.core.HiveType.parseHiveType(${Literal(Constant(hiveType.hiveType))}).toOption.get.asInstanceOf[com.rouesnel.typedsql.core.StructType]"
    )
  }
}
