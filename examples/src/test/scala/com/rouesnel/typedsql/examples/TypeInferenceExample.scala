package com.rouesnel.typedsql
package examples


//An example of a structure that does not compile with abstract classes inside
//CompiledSqlQuery but does with abstract types. Not asserting anything at
//runtime, just verifying compilation

abstract class TypedSqlFeatureSet[S <: CompiledSqlQuery](s: S) extends FeatureSet[S#Row]
trait Feature[S]
trait FeatureSet[S]  {
  def features: Iterable[Feature[S]]
}

object Step1FeatureSet extends TypedSqlFeatureSet(Step1) {
  val feat1: Feature[Step1.Row] = ???

  def features = List(feat1)
}
