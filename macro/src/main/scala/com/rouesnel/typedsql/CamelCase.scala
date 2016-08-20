package com.rouesnel.typedsql

object CamelCase {
  // Stolen from http://stackoverflow.com/a/1176023/49142
  def toUnderscores(camelCased: String) =
    camelCased
      .replaceAll("""(.)([A-Z][a-z]+)""", """$1_$2""")
      .replaceAll("""([a-z0-9])([A-Z])""", "$1_$2")
      .toLowerCase()
}
