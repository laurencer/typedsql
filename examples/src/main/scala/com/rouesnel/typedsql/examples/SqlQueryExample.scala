package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery object SqlQueryExample {

  @UDF def myUdf(input: Int): String = (0 to input).toList.mkString(",")

  @com.rouesnel.typedsql.UDF def myOtherUdf(input: Double): Int = ???

  def query(minimumAge: Int)
           (people: DataSource[com.rouesnel.typedsql.Person],
            test: DataSource[ManualSqlStruct],
            myPeople: DataSource[Person]
           ) =
    """
      SELECT 1 as int_value,
            1.3 as double_value,
            "string" as string_value,
            map("key", 1.0, "key2", 2) as map_value,
            struct(1.0, "stringvalue", 0) as struct_value,
            named_struct("field_name", 1, "field2", 2) as named_struct_value,
            cast(1 as boolean) as boolean_value,
            cast(1 as tinyint) as tiny_int_value,
            cast(1 as smallint) as small_int_value,
            myUdf(5) as udf_applied
      FROM ${people}
      WHERE age > ${minimumAge}
    """
  // TODO - array's don't work yet: array(1, 2, 3, 4) as arrayValue

}