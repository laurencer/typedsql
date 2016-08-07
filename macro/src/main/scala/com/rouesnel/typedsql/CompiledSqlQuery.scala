package com.rouesnel.typedsql

import java.util.Date

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}

import scala.util.Random
import scala.util.Random

object CompiledSqlQuery {
  def tmpDatabase     = "typedsql_tmp"
  def tmpTableName()  = new Date().getTime + "_" + Random.nextLong
  def tmpHdfsPath     = "/tmp"
}

trait CompiledSqlQuery {
  /** The HiveQL - note this must be a string literal */
  def query: String

  /** How to partition the output table (if at all) */
  def partitions: List[(String, String)] = Nil

  /** Source datasets inside the query */
  abstract class Sources

  /** Configuration parameters available inside the query */
  abstract class Parameters

  /** Output type for each row returned by the query */
  abstract class Row extends ThriftStruct
}

object SqlParameter {
  def write[T](value: T)(implicit writer: SqlParameter[T]) = writer.write(value)
  implicit def string = SqlParameter[String](value => "\"" + value + "\"")
  implicit def int    = SqlParameter[Int](value => value.toString)
}
case class SqlParameter[T](write: T => String)
