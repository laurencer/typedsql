package com.rouesnel.typedsql.macros

import com.rouesnel.typedsql.core._

import scala.reflect.api.Trees
import scala.reflect.macros.whitebox
import scalaz.{-\/, \/, \/-}

class SourceMapping[C <: whitebox.Context](val c: C) {
  import c.universe._

  def readSources(parameters: Seq[Trees#Tree]): (Seq[Trees#Tree], Map[String, StructType]) = {
    val processed = parameters.collect({
      case q"$mods val ${ name }: DataSource[${ objectType }]" => {
        val typeChecked = c.typecheck(objectType, c.TYPEmode)
        val tpe = Option(typeChecked.tpe.companion).getOrElse(
          c.abort(c.enclosingPosition, "Could not determine type of " + objectType.toString()))
        // Ensure its a ThriftStructCodec3
        if (!tpe.weak_<:<(
              c.typecheck(tq"com.twitter.scrooge.ThriftStructCodec3[_]", c.TYPEmode).tpe)) {
          c.abort(c.enclosingPosition, s"${objectType} must be a subtype of ThriftStructCodec3[_]")
        }

        val cleanedFields =
          new ThriftHiveTypeMacro[c.type](c).mapObjectTypeToHiveSchema(tpe)
        \/.right(name.toString -> cleanedFields)
      }
      case other => \/.left(other)
    })

    processed.collect({ case -\/(skipped) => skipped }) -> processed
      .collect({ case \/-(source)         => source })
      .toMap

  }
}
