package com.rouesnel.typedsql.hive

import java.io.FileOutputStream

import argonaut.Argonaut._
import argonaut._
import com.rouesnel.typedsql.core._
import com.rouesnel.typedsql.udf.UdfDescription
import com.rouesnel.typedsql.util._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.ql.Driver

import scala.language.experimental.macros
import scalaz._

/**
  * Provides a caching mechanism that persists Hive results to the target folder to improve
  * performance.
  */
object HiveCache {

  implicit def hiveTypeCodec: CodecJson[HiveType] =
    codec1(
      (str: String) =>
        HiveType
          .parseHiveType(str)
          .fold(err => throw new Exception(s"Could not parse: ${str} (${err})"), identity),
      (ht: HiveType) => ht.hiveType
    )("hive_type")

  implicit def structTypeCodec: CodecJson[StructType] =
    codec1(
      (str: String) =>
        HiveType
          .parseHiveType(str)
          .fold(err => throw new Exception(s"Could not parse: ${str} (${err})"), identity)
          .asInstanceOf[StructType],
      (ht: HiveType) => ht.hiveType
    )("hive_type")

  implicit def requestCodec = casecodec5(Request.apply _, Request.unapply _)(
    "sources",
    "parameters",
    "udfs",
    "query",
    "version"
  )

  case class Request(sources: Map[String, StructType],
                     parameterVariables: Map[String, String],
                     udfs: List[UdfDescription],
                     query: String,
                     version: Int = 0)

  implicit def compiledCodec =
    casecodec2(Compiled.apply _, Compiled.unapply _)("request", "compiled_schema")

  case class Compiled(request: Request, compiledSchema: List[(String, HiveType)])

  def cached(driver: Driver,
             hiveConf: HiveConf,
             sources: Map[String, StructType],
             parameterVariables: Map[String, String],
             udfs: List[UdfDescription],
             query: String,
             logger: String => Unit)(
      f: Schema => List[(String, HiveType)]): Throwable \/ List[(String, HiveType)] =
    \/.fromTryCatchNonFatal {
      val request        = Request(sources, parameterVariables, udfs, query)
      val pickledRequest = request.asJson.spaces2.getBytes()
      val hashedRequest =
        scala.util.hashing.MurmurHash3.bytesHash(pickledRequest)

      // Get the TypedSQL directory.
      val currentDir  = new java.io.File("").getAbsoluteFile
      val targetDir   = new java.io.File(currentDir, "target")
      val typedSqlDir = new java.io.File(targetDir, "typedsql")
      typedSqlDir.mkdirs()

      // Create the path to the cached directory.
      val cachedFile =
        new java.io.File(typedSqlDir, s"cache_${hashedRequest.toString.replace("-", "0")}.json")

      logger(s"Looking for cached query schema at ${cachedFile.getAbsolutePath}")

      // Try read the file if it exists. If reading fails or something else happens
      // fall back to generating from scratch.
      val cachedRead = if (cachedFile.exists()) {
        logger("Cached file exists - reusing existing result.")
        \/.fromTryCatchNonFatal(
            scala.io.Source
              .fromFile(cachedFile)
              .mkString
              .parse
              .flatMap(_.as[Compiled].result)
              .leftMap(details => new Exception(s"Could not parse ${cachedFile}: ${details}"))
          )
          .flatMap(identity)
          .fold(
            ex => {
              logger(s"Error reading cached query schema : ${cachedFile}\n${ExceptionString(ex)}")
              None
            },
            data => Some(data.compiledSchema)
          )
      } else {
        None
      }

      cachedRead match {
        case Some(schema) => schema
        case None => {
          logger("No cache exists for Hive query - evaluating...")
          val compiled = HiveQuery
            .compileQuery(driver, hiveConf, sources, parameterVariables, udfs, query)
            .fold(ex => throw ex, identity)

          try {
            cachedFile.createNewFile()
            val fos    = new FileOutputStream(cachedFile)
            val result = f(compiled)
            fos.write(Compiled(request, result).asJson.spaces2.getBytes)
            fos.close()
            result
          } catch {
            case ex: Exception => {
              cachedFile.delete()
              throw ex
            }
          }
        }
      }
    }

  def cached(hiveConf: HiveConf,
             sources: Map[String, StructType],
             parameterVariables: Map[String, String],
             udfs: List[UdfDescription],
             query: String,
             logger: String => Unit)(
      f: Schema => List[(String, HiveType)]): Throwable \/ List[(String, HiveType)] =
    HiveSupport.useHiveClassloader {
      \/.fromTryCatchNonFatal(new Driver(hiveConf))
        .flatMap(driver => {
          val result =
            cached(driver, hiveConf, sources, parameterVariables, udfs, query, logger)(f)
          driver.close()
          driver.destroy()
          result
        })
    }
}
