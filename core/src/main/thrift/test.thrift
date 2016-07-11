#@namespace scala com.rouesnel.typedsql

struct DateOfBirth {
 1: i32 day
 2: i32 month
 3: i64 year
}

struct Person {
  1: string firstname
  2: string lastname
  3: i32 age
}

struct ManualSqlStruct {
  1: i32 field_name
  2: i32 field2
}

struct ManualSqlQueryTypeTest {
  1: i32 intValue
  2: double doubleValue
  3: string stringValue
  // 3: map<string, i32> mapValue
  // 5: list<i32> arrayValue
}

struct NestedStructTest {
  1: i32 intValue
  2: double doubleValue
  3: string stringValue
  4: ManualSqlStruct manual
  5: map<string, i32> mapValue
  6: list<i32> arrayValue
}