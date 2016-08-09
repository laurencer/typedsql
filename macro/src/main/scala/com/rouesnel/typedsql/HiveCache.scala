package com.rouesnel.typedsql

import java.io.{FileInputStream, FileOutputStream}

import scala.language.experimental.macros

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.ql.Driver

import scalaz._
import Scalaz._

import com.rouesnel.typedsql.core._

import argonaut._, Argonaut._

/**
 * Provides a caching mechanism that persists Hive results to the target folder to improve
 * performance.
 */
object HiveCache {

  implicit def hiveTypeCodec: CodecJson[HiveType] = codec1(
    (str: String)   => HiveType.parseHiveType(str).fold(err => throw new Exception(s"Could not parse: ${str} (${err})"), identity),
    (ht: HiveType)  => ht.hiveType
  )("hive_type")

  implicit def structTypeCodec: CodecJson[StructType] = codec1(
    (str: String)   =>
      HiveType.parseHiveType(str).fold(err => throw new Exception(s"Could not parse: ${str} (${err})"), identity).asInstanceOf[StructType],
    (ht: HiveType)  => ht.hiveType
  )("hive_type")

  implicit def requestCodec = casecodec4(Request.apply _, Request.unapply _)(
    "sources",
    "parameters",
    "query",
    "version"
  )

  case class Request(sources: Map[String, StructType],
                     parameterVariables: Map[String, String],
                     query: String,
                     version: Int = 0)


  implicit def compiledCodec = casecodec2(Compiled.apply _, Compiled.unapply _)("request", "compiled_schema")

  case class Compiled(request: Request, compiledSchema: List[(String, HiveType)])

  def cached(driver: Driver, hiveConf: HiveConf, sources: Map[String, StructType], parameterVariables: Map[String, String], query: String)
            (f: Schema => List[(String, HiveType)]): List[(String, HiveType)] = {
    val request = Request(sources, parameterVariables, query)
    val pickledRequest = request.asJson.spaces2.getBytes()
    val hashedRequest = scala.util.hashing.MurmurHash3.bytesHash(pickledRequest)

    // Get the TypedSQL directory.
    val currentDir = new java.io.File(".").getAbsoluteFile
    val targetDir = new java.io.File(currentDir, "target")
    val typedSqlDir = new java.io.File(targetDir, "typedsql")
    typedSqlDir.mkdirs()

    // Create the path to the cached directory.
    val cachedFile = new java.io.File(typedSqlDir, s"cache_${hashedRequest.toString.replace("-","0")}.json")
    println(cachedFile)

    if (cachedFile.exists()) {
      println("Cached file exists - reusing existing result.")
      \/.fromTryCatchNonFatal(
        scala.io.Source
          .fromFile(cachedFile)
          .mkString
          .parse
          .flatMap(_.as[Compiled].result)
          .leftMap(details => new Exception(s"Could not parse ${cachedFile}: ${details}"))
      ).flatMap(identity).fold(
        err => throw new Exception(s"Error reading: ${cachedFile}", err),
        _.compiledSchema
      )
    } else {
      println("No cache exists for Hive query - evaluating...")
      val compiled = HiveQuery
        .compileQuery(driver, hiveConf, sources, parameterVariables, query)
        .fold(ex => throw ex, identity)

      cachedFile.createNewFile()
      val fos = new FileOutputStream(cachedFile)
      val result = f(compiled)
      fos.write(Compiled(request, result).asJson.spaces2.getBytes)
      fos.close()
      result
    }
  }

  def cached(hiveConf: HiveConf, sources: Map[String, StructType], parameterVariables: Map[String, String], query: String)
            (f: Schema => List[(String, HiveType)]): List[(String, HiveType)] = HiveSupport.useHiveClassloader {
    val driver = new Driver(hiveConf)
    try {
      cached(driver, hiveConf, sources, parameterVariables, query)(f)
    } finally {
      driver.close()
      driver.destroy()
    }
  }
}