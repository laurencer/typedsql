#@namespace scala com.rouesnel.typedsql.examples

struct ValueType {
  1: optional string string_value
  2: optional i16    short_value
  3: optional i32    int_value
  4: optional i64    long_value
  5: optional double double_value
  10: required i64   time_value
}

struct ComplexValue {
  1: required map<string, ValueType> data
  2: required string data_id
} ( partitions = 'a' )
