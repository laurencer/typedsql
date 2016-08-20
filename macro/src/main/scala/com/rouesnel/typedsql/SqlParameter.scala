package com.rouesnel.typedsql

object SqlParameter {
  def write[T](value: T)(implicit writer: SqlParameter[T]) =
    writer.write(value)

  // TODO: fix the broken interpolation.
  implicit def string =
    SqlParameter[String](value => "\"" + value + "\"")

  // Number types.
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
}

/** Typeclass that represent a type that can be used as a parameter to interpolate into
  * a Hive query.
  *
  * @param write function to interpolate/escape the value.
  * @tparam T type of value to interpolate.
  */
case class SqlParameter[T](write: T => String)
