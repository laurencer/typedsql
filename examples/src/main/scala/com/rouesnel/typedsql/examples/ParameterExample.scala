package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery
object ParameterExample {

  def query(bool: Boolean,
            byte: Byte,
            short: Short,
            int: Int,
            long: Long,
            float: Float,
            double: Double,
            date: java.sql.Date,
            string: String) =
    """
      SELECT ${bool}   as bool_value,
             ${byte}   as tiny_int_value,
             ${short}  as small_int_value,
             ${int}    as int_value,
             ${long}   as long_value,
             ${float}  as float_value,
             ${double} as double_value,
             ${string} as string_value,
             cast(${date} as string) as date_value
      """

}
