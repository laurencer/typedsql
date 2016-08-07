package com.rouesnel.typedsql.intellij

import java.io.File

import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._

import scala.collection.immutable.ListMap

import akka.actor._
import akka.pattern.ask

import com.intellij.openapi.diagnostic.Logger
import com.intellij.psi.PsiElement

import com.typesafe.config._

import org.jetbrains.plugins.scala.lang.psi.api.base.ScLiteral
import org.jetbrains.plugins.scala.lang.psi.api.statements.{ScFunctionDefinition, ScValue}
import org.jetbrains.plugins.scala.lang.psi.api.toplevel.templates.{ScExtendsBlock, ScTemplateBody}
import org.jetbrains.plugins.scala.lang.psi.api.toplevel.typedef._
import org.jetbrains.plugins.scala.lang.psi.impl.toplevel.typedef.SyntheticMembersInjector

import org.jetbrains.plugins.scala.lang.psi.ScalaPsiUtil
import org.jetbrains.plugins.scala.lang.psi.api.base.types.{ScParameterizedTypeElement, ScSimpleTypeElement}
import org.jetbrains.plugins.scala.lang.psi.types.result.TypingContext

import scalaz._
import Scalaz._

import com.rouesnel.typedsql.api._
import com.rouesnel.typedsql.core._

object CliHelper {
  import scala.sys.process._

  def startCliProcess(): Process = {
    val pathToCurrentClass = new File(
      classOf[Injector].getResource('/' + classOf[Injector].getName.replace('.', '/') + ".class")
          .getPath
          .drop(5) // "file:" prefix
    )

    val pathToCurrentJar = pathToCurrentClass.toString.split('!').head

    // Path to the plugin location
    val pluginLocation =
      new File(pathToCurrentJar) // <plugin location>/lib/<jar name>.jar
        .getParentFile // <plugin location>/lib
        .getParentFile // <plugin location>


    val binDirectory = new File(pluginLocation, "bin")

    val jars = binDirectory.listFiles().toList

    s"java -cp ${jars.mkString(":")} com.rouesnel.typedsql.intellij.server.CompilationServer".run()
  }
}

class Injector extends SyntheticMembersInjector {
  import scala.concurrent.ExecutionContext.Implicits.global

  val process = CliHelper.startCliProcess()

  // The injector runs a separate JVM to perform Hive schema compilation (prevents classpath issues).
  // Communication is performed using Akka Remoting.
  def useAkkaClassloader[T](f: => T): T = {
    // Replace the classloader with the correct path.
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(classOf[Actor].getClassLoader)
    // Run the computation
    val result = try {
      Right(f)
    } catch {
      case ex: Exception => Left(ex)
    }
    // Reset the contextClassLoader.
    Thread.currentThread().setContextClassLoader(contextClassLoader)
    // Return the result
    result match {
      case Right(value) => value
      case Left(error) => throw error
    }
  }

  val system = useAkkaClassloader { ActorSystem("intellij", ConfigFactory.parseString(
    """
      |akka {
      |  version = "2.4.8"
      |  loglevel = "INFO"
      |  actor {
      |    provider = "akka.remote.RemoteActorRefProvider"
      |  }
      |  remote {
      |    enabled-transports = ["akka.remote.netty.tcp"]
      |    netty.tcp {
      |      hostname = "127.0.0.1"
      |      port = 39115
      |    }
      |    log-sent-messages = on
      |    log-received-messages = on
      |  }
      |}
    """.stripMargin).withFallback(DefaultAkkaConfig.config).resolve()) }

  val remoteActor = system.actorSelection("akka.tcp://typedsql-intellij-server@127.0.0.1:39114/user/listener")
  val loggingActor = system.actorSelection("akka.tcp://typedsql-intellij-server@127.0.0.1:39114/user/logger")
  import akka.util.Timeout, Timeout._
  implicit val timeout: Timeout = 15 seconds

  def remoteLog(idx: Int, msg: Object): Unit = {
    val threadId = Thread.currentThread().getId
    loggingActor ! LogMessage(s"[${threadId}] ${idx}: " + msg.toString)
  }

  val defaultCaseClass = "case class Row()"
  val defaultScroogeTraitFields = Set(
    "notify",
    "productElement",
    "getClass",
    "productArity",
    "getFieldBlob",
    "finalize",
    "canEqual",
    "wait",
    "clone",
    "copy",
    "_passthroughFields",
    "write",
    "notifyAll",
    "getFieldBlobs",
    "productIterator",
    "setField",
    "productPrefix",
    "toString",
    "unsetField",
    "equals",
    "hashCode"
  )

  /**
   * Stores compilation requests that were completed successfully mapping from the fully
   * qualified name of the object to the used request. This allows the previously resolved
   * parameters and sources to be used if they are not available in this pass.
   */
  val fallbackCache = scala.collection.mutable.WeakHashMap[String, CompilationRequest]()

  /** Stores the last successful resolution for the given fully qualified name */
  val resolutionCache = scala.collection.mutable.WeakHashMap[String, StructType]()

  override def injectInners(source: ScTypeDefinition): Seq[String] = {
    source match {
      case o: ScObject if o.hasAnnotation("com.rouesnel.typedsql.SqlQuery").isDefined => {

        /** Returns an iterator of all the children elements in the objects body */
        def objectBody =
        // Loop through all children of the object (this is basically syntax).
          o.children.collect({
            // Extends block contains everything after any class extends.
            case eb: ScExtendsBlock => {
              eb.children.collect({
                // Template body actually contains classes, definitions, methods, etc.
                case td: ScTemplateBody => {
                  td.children
                }
              }).flatten
            }
          }).flatten


        /* Finds the Sources case class from the current class */
        val maybeSources = objectBody.collectFirst({
          // Try to find a class named Sources.
          case clazz: ScClass if clazz.name == "Sources" => {
            clazz.constructor
              .toRightDisjunction("Failed to find primary constructor")
              .map(ctor => {
                // Loop through each parameter for the class and process/resolve the types to
                // work out the tables/schemas we'll need in the compilation environment.
                val parameters = ctor.parameters.toList
                val definedSources = parameters.map(param => {
                  val calculatedSchema = param.typeElement.toRightDisjunction("Failed to get type element").flatMap({
                    // Each Source parameter should be a DataSource[T]
                    // Filter to only parameterised types.
                    case te: ScParameterizedTypeElement => {
                      // The inside of a DataSource[T] should only be simple types.
                      val schemaType = te.typeArgList.typeArgs.toList match {
                        case (ste: ScSimpleTypeElement) :: Nil => \/.right(ste)
                        case other => {
                          \/.left(s"Expected to get a single type parameter of the DataSource[T]. Instead found ${other}")
                        }
                      }

                      // Useful name used in debug messages.
                      def typeName = schemaType.map(_.getText).getOrElse("???")

                      // Try and resolve the body contents for the schema type.
                      val resolvedSchemaType =
                        schemaType
                          // Try and get a reference to the actual underlying nested type.
                          .flatMap(_.reference.toRightDisjunction(s"Failed to get reference for parameter type ${typeName}"))
                          // Try and resolve the actual schema type (e.g. gets it type def/body).
                          .flatMap(ref => {
                            Option(ref.resolve())
                              .toRightDisjunction(s"Failed to resolve type/schema for ${typeName}")
                          })

                      // If we did manage to resolve the type, now we collect all the methods
                      // from the class and use heuristics to work out what the thrift
                      // generated names/fields were (e.g. dropping known class parameters).
                      val fieldsInsideSchema =
                        resolvedSchemaType.flatMap({
                          case st: ScTrait => {
                            val thriftFields = st.getAllMethods.toList.filter(m =>
                              !defaultScroogeTraitFields.contains(m.getName)
                            ).filter(f => {
                              !(f.getName.startsWith("unset") || f.getName.startsWith("copy$") || f.getName.startsWith("_"))
                            }).reverse.map(m => (m.getName, m.getReturnType.getPresentableText))

                            \/.right(thriftFields)
                          }
                          /*case clazz: ScClass => {
                            remoteLog(12314432, "Got CLAZZZ - USING CLAZZ VIEW INSTEAD :)")
                            clazz.constructor.map(ctor => {
                              val parameters = ctor.parameters.toList
                              val fields = parameters.map(param => {
                                val typeName = param.typeElement.map({
                                  case ste: ScSimpleTypeElement => ste.getText
                                })
                                param.name -> typeName.getOrElse("???")
                              })
                              remoteLog(12314432, s"Got FIELDS: ${fields}")
                              fields
                            }).toRightDisjunction("Failed to find constructor for returned Row class")
                          }*/
                          case other => \/.left(s"Expected a Trait for schema but instead found: ${other}")
                        })
                      fieldsInsideSchema
                    }
                    case other => \/.left(s"Expected a DataSource[T] (a parameterized type element) but instead found: ${other}")
                  })

                  param.getName() -> calculatedSchema
                })

                // All the sources.
                definedSources
              })
            }
          }) match {
            case Some(sources) => sources.flatMap(srcs => {
              val successes = srcs.collect({ case (name, \/-(fields)) => (name, fields) })
              val failures  = srcs.collect({ case (name, -\/(errors)) => (name, errors) })
              val cached = failures.collect({
                case (name, errorMessage) if errorMessage.contains("Failed to resolve")  => {
                  val typeName = errorMessage.reverse.drop(4).reverse.split(" ").last
                  resolutionCache.get(typeName).map(struct => name -> struct)
                }
              }).flatten

              if (failures.nonEmpty && failures.length != cached.length) {
                \/.left(s"Failed to get Sources:\n\t${failures.map({ case (name, error) => s"- ${name}: ${error}"}).mkString("\n\t")}")
              } else {
                val converted = successes.map({ case (name, values) => {
                  name -> StructType(ListMap(values.map({
                    case (fieldName, fieldType) => {
                      fieldName -> (fieldType match {
                        case "int"    => IntType
                        case "double" => DoubleType
                        case "String" => StringType
                      })
                    }}): _*))
                }})

                \/.right(converted ++ cached)
              }
            })
            case None => {
              // Could not find Sources class - just return Nil
              \/.right(Nil)
            }
          }

        remoteLog(0, "Sources: " + maybeSources.toString)

        /* Finds the Parameters case class from the current class */
        val maybeParameters =
          objectBody.collectFirst({
            case clazz: ScClass if clazz.name == "Parameters" => {
              clazz.constructor.map(ctor => {
                val parameters = ctor.parameters.toList
                parameters.zipWithIndex.map({ case (param, idx) => {
                  val typeName = param.typeElement.map({
                    case ste: ScSimpleTypeElement => ste.getText
                  })
                  param.name -> typeName.getOrElse("???")
                }})
              }).toRightDisjunction("Failed to find constructor for Parameters class")
            }
          }).getOrElse(\/.right(Nil))

        remoteLog(0, "Parameters: " + maybeParameters.toString)

        val maybeSqlText =
          objectBody.collectFirst({
            case fd: ScFunctionDefinition if fd.name == "query" => {
              fd.children.collectFirst({
                case l: ScLiteral => l.getText
              })
            }
          }).flatten

        maybeSqlText match {
          // No query detected...
          case None => Nil
          case Some(sqlText) => {
              val validatedSources = maybeSources.validation.leftMap(_.wrapNel)
              val validatedParameters = maybeParameters.validation.leftMap(_.wrapNel)

              val validatedRequest = (validatedSources |@| validatedParameters) { (srcs, params) => {
                CompilationRequest(sqlText, params.toMap, srcs.toMap)
              }}

            validatedRequest.map(request => {
              try {
                fallbackCache.update(o.qualifiedName, request)
                val result = Await.result(remoteActor.ask(request), 1000.millis)
                result match {
                  case res@CompilationResponse(code, Some(schema)) => {
                    resolutionCache.put(o.qualifiedName, schema)
                    resolutionCache.put(o.name, schema)
                    remoteLog(999, s"Got successful code response: ${code}")
                    code.toList
                  }
                  case other => {
                    throw new Exception(s"Expected CompilationResponse but got: ${other}")
                  }
                }
              } catch {
                case ex: TimeoutException => {
                  remoteLog(101, s"TimeoutException caught...")
                  List(defaultCaseClass)
                }
              }
          }).fold(errors => {
              remoteLog(432423,   "Sources not available. Not trying to compile...")
              errors.foreach(err => remoteLog(432423, s"ERROR: ${err}"))
              // Reuse default if it exists.
              fallbackCache.get(o.qualifiedName).map(cr => {
                remoteLog(432423,   "Cached value available (reusing parameters + sources) :)")
                try {
                  val result = Await.result(remoteActor.ask(cr.copy(query = sqlText)), 1000.millis)
                  result match {
                    case res@CompilationResponse(code, Some(schema)) => {
                      resolutionCache.put(o.qualifiedName, schema)
                      resolutionCache.put(o.name, schema)
                      remoteLog(999, s"Got successful code response: ${code}")
                      code.toList
                    }
                    case other => {
                      throw new Exception(s"Expected CompilationResponse but got: ${other}")
                    }
                  }
                } catch {
                  case ex: TimeoutException => {
                    remoteLog(101, s"TimeoutException caught...")
                    List(defaultCaseClass)
                  }
              }}).getOrElse({
                remoteLog(432423,   "No cached value available..")
                List(defaultCaseClass)
              })
            }, identity)
          }
        }
      }
      case _ => Nil
    }
  }

  override def injectFunctions(source: ScTypeDefinition): Seq[String] = {
    source match {
      case o: ScObject if o.hasAnnotation("com.rouesnel.typedsql.SqlQuery").isDefined => {
        List("def apply(srcs: Sources, params: Parameters): com.rouesnel.typedsql.DataSource[Row] = ???")
      }
      case _ => Nil
    }
  }
}
