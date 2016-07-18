package com.rouesnel.typedsql

import scala.reflect.api.Trees
import scala.reflect.macros.whitebox

class SourceMapping(c: whitebox.Context) {
  import c.universe._

  def findSourceField(objectBody: Seq[Trees#Tree]) = {
    val sourceField = objectBody.collect({
      case q"def sources = ${sources}" => sources
    }).headOption
      .getOrElse(c.abort(c.enclosingPosition, "Must have a function called `source`."))

    val sources = sourceField match {
      // It's easy to get the args of a tuple constructor as a flat list - so we pattern
      // match it here.
      case Apply(Select(Ident(TermName("scala")), TermName(tupleConstructor)), args) => {
        // Ensure that a tuple has been used before looking at function args.
        if (tupleConstructor.startsWith("Tuple")) {
          args.map({
            case q"${Literal(Constant(name))} -> ${objectType}" => {
              println(s"${name} -> ${objectType}")
              val typeChecked = c.typecheck(objectType, c.TYPEmode)
              val tpe = Option(typeChecked.tpe).getOrElse(c.abort(c.enclosingPosition, "Could not determine type of " + objectType.toString()))
              // Ensure its a ThriftStructCodec3
              if (! tpe.weak_<:<(c.typecheck(tq"com.twitter.scrooge.ThriftStructCodec3[_]", c.TYPEmode).tpe)) {
                c.abort(c.enclosingPosition, s"${objectType} must be a subtype of ThriftStructCodec3[_]")
              }
              // Now we can extract all relevant fields and reverse a schema.
              // 1. Extract the val {name}Field = new TField(...) methods
              val fieldNamesFromCompanion = tpe.members.toList.collect({
                case m: MethodSymbol if m.name.toString.endsWith("Field") && (! m.isPrivate) => {
                  val fieldName = m.name.toString
                  fieldName.substring(0, fieldName.length - "Field".length)
                }
              })

              println(fieldNamesFromCompanion)

              val underlyingClass = tpe.baseClasses.collect({
                case c: ClassSymbol if c.name == TypeName("ThriftStructCodec3") =>
                  c.typeParams
              }).headOption
              println(underlyingClass)
              println(tpe.baseClasses.toList)

            }
            case other => c.abort(c.enclosingPosition, "A string literal must be used for the mapping name with the arrow operator (e.g. `\"my_table\" -> MyTableRow`. Instead found: " + other.toString())
          })
        } else {
          c.abort(c.enclosingPosition, "Source function must be a tuple (not a list). Instead found: " + tupleConstructor)
        }
      }
      case _ => c.abort(c.enclosingPosition, "Source function must be a tuple of (String -> Object tuples).")
    }

    c.abort(c.enclosingPosition, "EOF.")
  }

}