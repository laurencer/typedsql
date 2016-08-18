package com.rouesnel.typedsql

import com.rouesnel.typedsql.core._

import com.twitter.scrooge.ThriftStruct

import scala.reflect.macros._

case class HasStructType[T](structType: StructType)

object HasStructType {
  implicit def materialiseHasStructType[T <: ThriftStruct]: HasStructType[T] = macro impl[T]


  def impl[T](c: Context)(implicit typ: c.WeakTypeTag[T]): c.Expr[HasStructType[T]] = {
    import c.universe._


    println(c.enclosingPosition)

    println(s"RUNNING ${typ}")


    val hiveType = new ThriftHiveTypeMacro[c.type](c)
      .convertScalaToHiveType(c.weakTypeOf[T])

    val result =
      q"com.rouesnel.typedsql.HasStructType[${typ}](com.rouesnel.typedsql.core.HiveType.parseHiveType(${Literal(Constant(hiveType.hiveType))}).toOption.get.asInstanceOf[com.rouesnel.typedsql.core.StructType])"

    println(result)

    c.Expr[HasStructType[T]](result)
  }
}