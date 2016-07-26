package com.rouesnel.typedsql.intellij.server

import akka.actor._
import akka.event.Logging

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.convert.decorateAsScala._
import com.rouesnel.typedsql._
import com.rouesnel.typedsql.api.{CompilationRequest, CompilationResponse, LogMessage}
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.Driver

import scala.collection.immutable.ListMap
import scalaz._

object CompilationServer extends App {
  implicit val system = ActorSystem("typedsql-intellij-server")
  val listener  = system.actorOf(Props[Listener], name = "listener")
  val logger    = system.actorOf(Props[Logger], name = "logger")
}

class Logger extends Actor {
  val log = Logging(context.system, this)
  def receive = {
    case msg: LogMessage => {
      log.info(msg.message)
    }
  }
}

/** Actor responsible for compiling Hive queries directly */
class Compiler extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global

  val log = Logging(context.system, this)

  val conf = Await.result(HiveSupport.initialize(true, println(_)), 15.seconds)

  val driver = new Driver(conf)

  def receive = {
    case cr: CompilationRequest => {
      val result = \/.fromTryCatchNonFatal({
        val rawSqlText = cr.query.trim.replaceAll("\\n", " ")
        /** IntelliJ includes the surrounding quotes. This removes them. */
        val sqlText = rawSqlText.dropWhile(_ == '"').reverse.dropWhile(_ == '"').reverse

        val parametersWithDefaults = cr.parameters.mapValues({
          case "Int"    => "0"
          case "Double" => "0.0"
          case "String" => "\"\""
          case other    => {
            log.error(s"Could not find type: ${other}. Assuming empty string.")
            "\"\""
          }
        })

        val mappedSources = cr.sources.mapValues(s => Converter.apiTypeToHiveType(s).asInstanceOf[StructType])

        log.info(s"Parameters: ${parametersWithDefaults}")
        log.info(s"Sources: ${mappedSources}")
        log.info(s"Query: ${sqlText}")

        HiveQuery.compileQuery(driver, conf, mappedSources, parametersWithDefaults, sqlText).flatMap(Converter.produceCaseClass(_, "Row")).fold(
          exception  => {
            log.error(exception, "Hive Query failed...")
            throw exception
          },
          (values) => {
            log.info(s"Generated: ${values}")
            CompilationResponse(Some(values._1), Some(values._2))
          }
        )
      })

      result.fold(
        error    => {
          log.error(error, "Hive Query failed...")
          sender ! (cr, CompilationResponse(None, None))
        },
        response => {
          log.info(s"Generated Case Class: ${response}")
          sender ! (cr, response)
        }
      )
    }
  }
}

/** Listens for requests and dispatches them to the compilers (handles caching, etc) */
class Listener extends Actor {
  val log = Logging(context.system, this)

  val inflight = scala.collection.mutable.HashMap[CompilationRequest, List[ActorRef]]()
  val cache = scala.collection.mutable.WeakHashMap[CompilationRequest, CompilationResponse]()

  val compiler = context.actorOf(Props[Compiler]())

  def receive = {
    // 1. Get a request from IntelliJ
    case cr: CompilationRequest => {
      cache.get(cr) match {
        case Some(response) => sender ! response
        case None => {
          // Add the current sender to the dispatch list.
          val currentWaitList = inflight.getOrElseUpdate(cr, Nil)
          inflight.update(cr, sender +: currentWaitList)

          // Send the request to the compiler.
          compiler ! cr
        }
      }
    }
    // 2. Get a response from the compiler (send back to waiting listeners)
    case (cr: CompilationRequest, resp: CompilationResponse) => {
      val waitingRequestors = inflight.remove(cr).getOrElse(Nil)
      cache.put(cr, resp)
      waitingRequestors.foreach(requestor => {
        requestor ! resp
      })
    }
  }
}

object Converter {
  def apiTypeToHiveType(apiType: api.HiveType): HiveType = apiType match {
    case api.PrimitiveType("Integer")     => PrimitiveType[Int]
    case api.PrimitiveType("Double")  => PrimitiveType[Double]
    case api.PrimitiveType("String")  => PrimitiveType[String]
    case m: api.MapType               => MapType(apiTypeToHiveType(m.key), apiTypeToHiveType(m.value))
    case l: api.ArrayType             => ArrayType(apiTypeToHiveType(l.valueType))
    case s: api.StructType            => StructType(ListMap(s.fields.mapValues(apiTypeToHiveType).toList: _*))
  }

  def hiveTypeToApiType(hiveType: HiveType): api.HiveType = hiveType match {
    case p: PrimitiveType[_] => api.PrimitiveType(p.scalaTypeName)
    case m: MapType          => api.MapType(hiveTypeToApiType(m.key), hiveTypeToApiType(m.value))
    case l: ArrayType        => api.ArrayType(hiveTypeToApiType(l.valueType))
    case s: StructType       => api.StructType(ListMap(s.fields.mapValues(hiveTypeToApiType).toList: _*))
  }

  def hiveTypeToScalaType(hiveType: HiveType): String = hiveType match {
    case p: PrimitiveType[_] => p.scalaTypeName
    case m: MapType          => s"scala.collection.Map[${hiveTypeToScalaType(m.key)}, ${hiveTypeToScalaType(m.value)}]"
    case l: ArrayType        => s"scala.collection.List[${hiveTypeToScalaType(l.valueType)}]"
    case s: StructType       => {
      val structuralTypeFields = s.camelCasedFields.toList.map({ case (fieldName, hiveType) => {
        s"def ${fieldName}: ${hiveTypeToScalaType(hiveType)}"
      }}).mkString("; ")
      s"{ ${structuralTypeFields} }"
    }
  }

  def produceCaseClass(schema: Schema, className: String = "Row"): Throwable \/ (String, api.StructType) = \/.fromTryCatchNonFatal {
    assert(schema != null, "Schema cannot be null...")
    assert(schema.getFieldSchemas != null, "Field schemas cannot be null...")
    // Get all of the fields in the row schema.
    val outputRecordFields = schema.getFieldSchemas.asScala.map(fieldSchema => {
      val fieldName = fieldSchema.getName
      val fieldType =
        HiveType.parseHiveType(fieldSchema.getType)
          .fold(missingType => throw new Exception(s"Missing type: $missingType"), identity)
      (fieldName, fieldType)
    })

    // Map all nested fields on the top-level schema to a struct
    // E.g. SELECT * FROM test1 INNER JOIN test2
    // will return test1.foo, test1.bar, test2.baz, test2.bax
    //
    // test1 and test2 should form reusable structs.
    val implicitStructs = outputRecordFields
      .collect({ case (fieldName, fieldType) if fieldName.contains(".") => {
        val fieldParts = fieldName.split("\\.").toList
        (fieldParts.dropRight(1), (fieldParts.last, fieldType))
      }})
      .groupBy({ case (parent, (fieldName, fieldType)) => parent })
      .mapValues(_.map({ case (parent, fieldInfo) => fieldInfo }))
      .mapValues(fields => StructType(ListMap(fields: _*)))

    // Map the nested structs to the generated ones and remove any nested fields
    val fieldsToGenerate = StructType(ListMap(outputRecordFields
      .filter({ case (fieldName, _) => !fieldName.contains(".") }) ++
      implicitStructs.toList.map({ case (fieldNameParts, fieldStruct) =>
        fieldNameParts.mkString(".") -> fieldStruct
      }): _*))

    val caseClassFields = fieldsToGenerate.camelCasedFields.toList.map({ case (fieldName, fieldType) => {
      s"${fieldName}: ${hiveTypeToScalaType(fieldType)}"
    }}).mkString(", ")

    val apiHiveType = hiveTypeToApiType(fieldsToGenerate).asInstanceOf[api.StructType]

    s"case class ${className}(${caseClassFields})" -> apiHiveType
  }
}
