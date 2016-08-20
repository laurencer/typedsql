package com.rouesnel.typedsql.core

import scala.reflect.macros._

/** From a generic Scala Type - produces a Hive Types*/
object GenerateHiveType {
  def apply[T] = macro impl[T]

  def impl[T](c: Context)(implicit typ: c.WeakTypeTag[T]): c.Expr[HiveType] = {
    import c.universe._

    val hiveType = new ThriftHiveTypeMacro[c.type](c)
      .convertScalaToHiveType(c.weakTypeOf[T])

    c.Expr[HiveType](
      q"com.rouesnel.typedsql.core.HiveType.parseHiveType(${Literal(Constant(hiveType.hiveType))}).toOption.get"
    )
  }
}
