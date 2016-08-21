package com.rouesnel.typedsql.core

import scala.reflect.macros._

class PartitionMapping[C <: whitebox.Context](val c: C) {
  import c.universe._

  val hiveType = new ThriftHiveTypeMacro[c.type](c)

  /** Converts a type reference into a list of partitions */
  def readPartitions(typ: Type): List[(String, HiveType)] = {
    typ.normalize match {
      case RefinedType(base, methods) => {
        methods.toList.collect({
          case m if m.isMethod => hiveType.camelCaseToUnderscores(m.name.encodedName.toString) -> hiveType.convertScalaToHiveType(m.asMethod.returnType)
        })
      }
      case unitType if unitType =:= typeOf[Unit] => Nil
      case other => {
        c.abort(c.enclosingPosition, s"Invalid type given for partitions: ${typ}")
      }
    }
  }
}
