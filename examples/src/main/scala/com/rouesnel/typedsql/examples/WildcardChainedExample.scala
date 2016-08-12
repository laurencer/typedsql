package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery object WildcardExample {

  case class Sources(people: DataSource[Person])
  case class Parameters()

  def query = "SELECT 1 as test_int, * FROM ${people}"
}

@SqlQuery object WildcardChainedExample {

  case class Sources(wildcard: DataSource[WildcardExample.Row])
  case class Parameters()

  def query = "SELECT 1 as test_int2, * FROM ${wildcard}"
}

@SqlQuery object WildcardExplicitExample {

  case class Sources(people: DataSource[Person])
  case class Parameters()

  def query = "SELECT 1 as test_int, p.* FROM ${people} p"
}

@SqlQuery object WildcardExplicitChainedExample {

  case class Sources(wildcard: DataSource[WildcardExplicitExample.Row])
  case class Parameters()


  def query = "SELECT 1 as test_int2, * FROM ${wildcard}"
}
