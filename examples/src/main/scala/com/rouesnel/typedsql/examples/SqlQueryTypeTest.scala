package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.{DataSource, ManualSqlStruct, Person, SqlQuery}

@SqlQuery object SqlQueryExample {

  case class Sources(people: DataSource[com.rouesnel.typedsql.Person],
                     test: DataSource[ManualSqlStruct],
                     myPeople: DataSource[Person]
                    )

  case class Parameters(minimumAge: Int)

  def query =
    """
      SELECT 1 as int_value,
            1.3 as double_value,
            "string" as string_value,
            map("key", 1.0, "key2", 2) as map_value,
            struct(1.0, "stringvalue", 0) as struct_value,
            named_struct("field_name", 1, "field2", 2) as named_struct_value
      FROM ${people}
      WHERE age > ${minimumAge}
    """
  // TODO - array's don't work yet: array(1, 2, 3, 4) as arrayValue

}