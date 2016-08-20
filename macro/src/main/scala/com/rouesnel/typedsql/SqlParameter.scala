package com.rouesnel.typedsql

import org.apache.hive.common.util.HiveStringUtils

object SqlParameter {
  def write[T](value: T)(implicit writer: SqlParameter[T]) =
    writer.write(value)

  // TODO: fix the broken interpolation.
  def escapeString(str: String, escapeChar: Char, specialCharacters: String): String = {
    val result = new StringBuilder()
    for (idx <- 0 until str.size) {
      val current = str.charAt(idx)
      if (current == escapeChar || specialCharacters.contains(current)) {
        result.append(escapeChar)
      }
      result.append(current)
    }
    result.toString
  }

  implicit def string =
    SqlParameter[String](value => "\"" + escapeString(value, '\\', "\"\\") + "\"")

  // Number types.
  implicit def byte =
    SqlParameter[Byte](_.toString)
  implicit def short =
    SqlParameter[Short](_.toString)
  implicit def int =
    SqlParameter[Int](_.toString)
  implicit def long =
    SqlParameter[Long](_.toString)
  implicit def float =
    SqlParameter[Float](_.toString)
  implicit def double =
    SqlParameter[Double](_.toString)

  // Misc types
  implicit def bool =
    SqlParameter[Boolean](_.toString)
  implicit def date =
    SqlParameter[java.sql.Date](d => s"cast('${d.toString}' as date)")
}

/** Typeclass that represent a type that can be used as a parameter to interpolate into
  * a Hive query.
  *
  * @param write function to interpolate/escape the value.
  * @tparam T type of value to interpolate.
  */
case class SqlParameter[T](write: T => String)
