package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery
object WildcardExample {
  def query(people: Unpartitioned[Person]) =
    "SELECT 1 as test_int, * FROM ${people}"
}

@SqlQuery
object WildcardChainedExample {
  def query(wildcard: WildcardExample.DataSource) =
    "SELECT 1 as test_int2, * FROM ${wildcard}"
}

@SqlQuery
object WildcardExplicitExample {
  def query(people: Unpartitioned[Person]) =
    "SELECT 1 as test_int, p.* FROM ${people} p"
}

@SqlQuery
object WildcardExplicitChainedExample {
  def query(wildcard: WildcardExplicitExample.DataSource) =
    "SELECT 1 as test_int2, * FROM ${wildcard}"
}
