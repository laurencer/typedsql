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