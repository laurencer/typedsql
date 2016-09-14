package com.rouesnel.typedsql

import com.twitter.scrooge.ThriftStruct

trait CompiledSqlQuery {

  /** The HiveQL - note this must be a string literal */
  def sql: String

  /** How to partition the output table (if at all) */
  def partitions: List[(String, String)] = Nil

  /** Source datasets inside the query */
  type Sources

  /** Configuration parameters available inside the query */
  type Parameters

  /** Output type for each row returned by the query */
  type Row <: ThriftStruct
}
