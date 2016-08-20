package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

/*

Unfortunately there isn't a nice way to test that conflicting wildcards are resolved correctly
without breaking the compiler :(

@SqlQuery object ConflictingWildcardSourceA {

  case class Sources(people: DataSource[Person])
  case class Parameters()

  def query = "SELECT 1 as test_conflict, p.* FROM ${people} p"
}

@SqlQuery object ConflictingWildcardSourceB {

  case class Sources(people: DataSource[Person])
  case class Parameters()

  def query = "SELECT 'value' as test_conflict2, p1.* FROM ${people} p1"
}

@SqlQuery object ConflictingWildcardExample {

  case class Sources(wildcardA: DataSource[ConflictingWildcardSourceA.Row],
                     wildcardB: DataSource[ConflictingWildcardSourceB.Row])
  case class Parameters()

  def query =
    """
       SELECT * FROM ${wildcardA} a INNER JOIN ${wildcardB} b ON a.firstname = b.firstname
    """
}
 */
