package com.rouesnel.typedsql.util

object CamelCase {

  /**
    * Converts a camel-cased string to underscores based on some simple heuristics
    * (stolen from http://stackoverflow.com/a/1176023/49142)
    */
  def toUnderscores(camelCased: String) =
    camelCased
      .replaceAll("""(.)([A-Z][a-z]+)""", """$1_$2""")
      .replaceAll("""([a-z0-9])([A-Z])""", "$1_$2")
      .toLowerCase()
}
