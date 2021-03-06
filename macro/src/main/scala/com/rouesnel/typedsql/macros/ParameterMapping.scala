package com.rouesnel.typedsql.macros

import com.rouesnel.typedsql.core.ScalaHiveTypeMapping

import scala.reflect.api.Trees
import scala.reflect.macros._

class ParameterMapping[C <: Context](val c: C) {
  import c.universe._

  val scalaTypes = new ScalaHiveTypeMapping[c.type](c)
  import scalaTypes._

  type FieldName    = String
  type DefaultValue = String

  def readParameters(params: Seq[Trees#Tree]): Map[FieldName, DefaultValue] = {
    params
      .collect({
        case q"$mods val ${ name }: ${ objectType }" => {
          val typeChecked = c.typecheck(objectType, c.TYPEmode)
          val tpe = Option(typeChecked.tpe).getOrElse(
            c.abort(c.enclosingPosition, "Could not determine type of " + objectType.toString()))
          val sqlParameterImplicit = try {
            c.typecheck(q"implicitly[com.rouesnel.typedsql.SqlParameter[${tpe}]]", c.TERMmode)
          } catch {
            case ex: TypecheckException =>
              c.abort(
                c.enclosingPosition,
                s"SqlParameter[${objectType}] implicit could not be found and is required for the parameter ${name}.")
          }

          val defaultValue = tpe match {
            case typ if (typ <:< booleanType) => "false"
            case typ if (typ <:< byteType)    => "0"
            case typ if (typ <:< shortType)   => "0"
            case typ if (typ <:< intType)     => "0"
            case typ if (typ <:< longType)    => "0"
            case typ if (typ <:< floatType)   => "0.0"
            case typ if (typ <:< doubleType)  => "0.0"
            case typ if (typ <:< dateType)    => "cast('2015-05-11' as date)"
            case typ if (typ <:< stringType)  => "\"\""
            case other => {
              c.abort(
                c.enclosingPosition,
                s"Encountered unknown SqlParameter[${objectType}] - using an empty string as a placeholder value...")
            }
          }

          name.toString() -> defaultValue
        }
      })
      .toMap
  }
}
