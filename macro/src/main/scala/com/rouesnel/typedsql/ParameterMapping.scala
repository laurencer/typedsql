package com.rouesnel.typedsql

import scala.reflect.api.Trees
import scala.reflect.macros.{TypecheckException, whitebox}

class ParameterMapping(c: whitebox.Context) {
  import c.universe._

  private val intType    = c.weakTypeOf[Int]
  private val doubleType = c.weakTypeOf[Double]
  private val stringType = c.weakTypeOf[String]

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
            case typ if (typ <:< doubleType) => "0.0"
            case typ if (typ <:< intType)    => "0"
            case typ if (typ <:< stringType) => "\"\""
            case other => {
              println(
                s"Encountered unknown SqlParameter[${objectType}] - using an empty string as a placeholder value...")
              "\"\""
            }
          }

          name.toString() -> defaultValue
        }
      })
      .toMap
  }
}
