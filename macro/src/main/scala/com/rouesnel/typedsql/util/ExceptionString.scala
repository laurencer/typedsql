package com.rouesnel.typedsql.util

import java.io.PrintStream

object ExceptionString {
  def apply(ex: Throwable, limit: Int = 5): String = {
    var e  = ex
    val sb = new StringBuilder()

    sb.append(e.toString + "\n")
    e.getStackTrace.take(limit).foreach(e1 => sb.append(s"\t at ${e1}\n"))
    while (e.getCause() != null) {
      e = e.getCause()
      sb.append(s"Caused by: ${e.toString}\n")
      e.getStackTrace.take(limit).foreach(e1 => sb.append(s"\t at ${e1.toString}\n"))
    }
    sb.toString()
  }
}
