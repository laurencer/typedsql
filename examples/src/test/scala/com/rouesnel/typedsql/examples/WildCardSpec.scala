package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._
import com.rouesnel.typedsql.test._

class WildCardSpec extends TypedSqlSpec { def is = sequential ^ s2"""
  Wildcard Example $wildcardExample
  Wildcard Chained Example $wildcardChainedExample

  Wilcard Explicit Example $wildcardExplicitExample
  Wildcard Explicit Chained Example $wildcardExplicitChainedExample

  Conflicting Example $conflictingWildcardExample
"""

  def people = createDataSource(Person("Bob", "Brown", 28))

  def wildcardExample = {
    val ds = WildcardExample.query(people)

    val result = executeDataSource(ds)

    println()
    println(result)
    println()

    // Check we have the fields
    result.head.firstname
    result.head.lastname
    result.head.age
    result.head.testInt

    ok
  }

  def wildcardChainedExample = {
    val wc = WildcardExample.query(people)
    val ds = WildcardChainedExample.query(wc)

    val result = executeDataSource(ds)

    println()
    println(result)
    println()

    // Check we have the fields
    result.head.firstname
    result.head.lastname
    result.head.age
    result.head.testInt
    result.head.testInt2

    ok
  }
  def wildcardExplicitExample = {
    val ds = WildcardExplicitExample.query(people)
    val result = executeDataSource(ds)

    println()
    println(result)
    println()

    // Check we have the fields
    result.head.firstname
    result.head.lastname
    result.head.age
    result.head.testInt

    ok
  }
  def wildcardExplicitChainedExample = {
    val wc = WildcardExplicitExample.query(people)

    val ds = WildcardExplicitChainedExample.query(wc)

    val result = executeDataSource(ds)

    println()
    println(result)
    println()

    // Check we have the fields
    result.head.firstname
    result.head.lastname
    result.head.age
    result.head.testInt
    result.head.testInt2

    ok
  }

  def conflictingWildcardExample = {
    /*
    See note in the ConflictingWildcardExample object...

    val wa = ConflictingWildcardSourceA(
      ConflictingWildcardSourceA.Sources(people),
      ConflictingWildcardSourceA.Parameters()
    )

    val wb = ConflictingWildcardSourceB(
      ConflictingWildcardSourceB.Sources(people),
      ConflictingWildcardSourceB.Parameters()
    )

    val ds = ConflictingWildcardExample(
      ConflictingWildcardExample.Sources(wa, wb),
      ConflictingWildcardExample.Parameters()
    )

    val result = executeDataSource(ds)

    println()
    println(result)
    println()

    // Check we have the fields
    result.head.firstname
    result.head.lastname
    result.head.age
    result.head.testConflict
    */
    ok
  }
}
