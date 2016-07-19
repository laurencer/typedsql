package com.rouesnel.typedsql

import java.util.Date

import com.twitter.scrooge.ThriftStructCodec3

import scala.util.Random
import scala.util.Random

object CompiledSqlQuery {
  def tmpDatabase     = "typedsql_tmp"
  def tmpTableName()  = new Date().getTime + "_" + Random.nextLong
  def tmpHdfsPath     = "/tmp"
}

trait CompiledSqlQuery {
  import CompiledSqlQuery._

  /** A tuple of tuples */
  def sources: Sources

  /** The HiveQL - note this must be a string literal */
  def query: String

  /** How to partition the output table (if at all) */
  def partitions: List[(String, String)] = Nil

  /** Source datasets inside the query */
  type Sources = Map[String, ThriftStructCodec3[_]]

  /** Configuration parameters available inside the query */
  type Configuration

  /** Output type for each row returned by the query */
  type Row

  /*
  def toTypedPipe(): Hive[TypedPipe[Row]] = {
    val tableName = tmpTableName()
   Hive.query(
     s"""
       |CREATE TABLE ${tmpDatabase}.${tableName}
       |STORED AS PARQUET
       |LOCATION '${tmpHdfsPath}/${tableName}'
       |AS $query
     """.stripMargin
   ).rMap(result => {
     TypedPipe.from(ParquetScroogeSource[Row](
       s"${tmpHdfsPath}/${tableName}"

     ))
   })
    ???
  }
  */
}
