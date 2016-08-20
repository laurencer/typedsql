package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery
object WildcardExample {
  def query(people: DataSource[Person]) =
    "SELECT 1 as test_int, * FROM ${people}"
}

@SqlQuery
object WildcardChainedExample {
  def query(wildcard: DataSource[WildcardExample.Row]) =
    "SELECT 1 as test_int2, * FROM ${wildcard}"
}

@SqlQuery
object WildcardExplicitExample {
  def query(people: DataSource[Person]) =
    "SELECT 1 as test_int, p.* FROM ${people} p"
}

@SqlQuery
object WildcardExplicitChainedExample {
  def query(wildcard: DataSource[WildcardExplicitExample.Row]) =
    "SELECT 1 as test_int2, * FROM ${wildcard}"
}
