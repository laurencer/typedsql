package com.rouesnel.typedsql

import java.io.File

import scala.language.experimental.macros
import scala.reflect.macros.Context

object FromFile {
  /**
   * Gets a resource in the Scala project at compile-time (checking that the file exists).
   *
   * The lookup is performed from the directory SBT is started within (e.g. the root of the
   * project).
   */
  def apply(path: String): String = macro applyImpl

  def applyImpl(c: Context)(path: c.Expr[String]) = {
    import c.universe._

    path.tree match {
      case Literal(Constant(s: String)) => {
        val file = new File(s)
        if (! file.exists()) {
          c.abort(c.enclosingPosition, s"File at path does not exist: ${file.getAbsolutePath}")
        } else if (! file.isFile) {
          c.abort(c.enclosingPosition, s"Path given is actually a directory: ${file.getAbsolutePath}")
        } else {
          c.literal(io.Source.fromFile(file).getLines().mkString("\n"))
        }
      }
      case _ => c.abort(c.enclosingPosition, "Need a literal path!")
    }
  }
}

