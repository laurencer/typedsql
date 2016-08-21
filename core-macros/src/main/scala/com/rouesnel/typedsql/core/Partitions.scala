package com.rouesnel.typedsql.core

import scala.reflect.macros._

object Partitions {
  type None = Unit
  implicit def nonePartitions: Partitions[None] = Partitions[None](Nil)

  implicit def materialisePartitions[T]: Partitions[T] = macro typeMacro[T]

  def typeMacro[T](c: Context)(implicit typ: c.WeakTypeTag[T]): c.Expr[Partitions[T]] = {
    import c.universe._

    val hiveTypeMacro = new ThriftHiveTypeMacro[c.type](c)
    val partitions = new PartitionMapping[c.type](c).readPartitions(typ.tpe)

    val variables = partitions.map({ case (name, partType) => {
      q"${Literal(Constant(name))} -> com.rouesnel.typedsql.core.HiveType.parseHiveType(${Literal(
        Constant(partType.hiveType))}).toOption.get"

    }})

    c.Expr[Partitions[T]](
      q"com.rouesnel.typedsql.core.Partitions[${typ}](List(..$variables))"
    )
  }
}
case class Partitions[T](fields: List[(String, HiveType)])

