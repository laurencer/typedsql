package com.rouesnel.typedsql

import scala.annotation.compileTimeOnly
import scala.annotation.StaticAnnotation
import scala.collection.convert.decorateAsScala._
import scala.collection.immutable.ListMap
import scala.concurrent._
import duration._
import scala.language.experimental.macros
import scala.reflect.macros._
import scalaz._
import Scalaz._
import com.rouesnel.typedsql.core._
import com.rouesnel.typedsql.hive.{HiveCache, HiveSupport}
import com.rouesnel.typedsql.macros.{ParameterMapping, ScroogeGenerator, SourceMapping, UDFMapping}
import com.rouesnel.typedsql.util._

@compileTimeOnly("enable macro paradise to expand macro annotations")
class SqlQuery extends StaticAnnotation {
  def macroTransform(annottees: Any*) = macro SqlQuery.impl
}

object SqlQuery {
  import au.com.cba.omnia.beeswax.Hive
  import org.apache.hadoop.hive.ql.Driver
  import org.apache.hadoop.hive.ql.session.SessionState

  val futureHiveConf = HiveSupport.initialize()
  def hiveConf       = Await.result(futureHiveConf, 25.seconds)

  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    val sourceMapping    = new SourceMapping(c)
    val parameterMapping = new ParameterMapping[c.type](c)
    val udfMapping       = new UDFMapping[c.type](c)

    val result = {
      annottees.map(_.tree).toList match {
        case q"$mods object $tpname extends ..$parents { ..$stats }" :: Nil => {
          val (queryParams, sqlLiteral) = stats
            .collect({
              // When updating here - you also need to change remainingMembers below
              case q"$mods def query(...${ paramss }) = $sqlQuery" =>
                (paramss, sqlQuery)
              case q"def query = $sqlQuery" => (Seq.empty, sqlQuery)
            })
            .headOption
            .map({ case (params, query) => params -> c.typecheck(query) })
            .getOrElse(c.abort(c.enclosingPosition, "Must have a function called `query()`.")) match {
            case (params, Literal(Constant(code))) => params -> code
            case _ =>
              c.abort(c.enclosingPosition, "Query function must be a literal string.")
          }

          // When updating here - you also need to change the collect function above
          val remainingMembers = stats.filter({
            case q"$mods def query(...${ paramss }) = $sqlQuery" => false
            case q"def query = $sqlQuery"                        => false
            case _                                               => true
          })

          val sqlStatement = sqlLiteral.toString.trim
          if (sqlStatement.split(";").length != 1) {
            c.abort(
              c.enclosingPosition,
              s"Only a single SQL statement is supported. Please remove any ';'s from $sqlLiteral")
          }

          // Get sources and create temp tables mapping to them.
          // This also returns a list of parameters that were not sources (for
          // processing as regular variables).
          val (nonSourceParameters, sources) =
            sourceMapping.readSources(queryParams.flatten)

          // Get/parse the remaining parameters.
          val parameters = parameterMapping.readParameters(nonSourceParameters)

          // Generate the fullly qualified name of this object (used to uniquely identify UDFs)
          val objectName = c.internal.enclosingOwner.fullName + "." + tpname.toString()

          // Get any UDFs.
          val udfs = udfMapping.readUDFs(objectName, stats)
          val udfDescriptions = udfs.map({
            case (description, _) => description
          })
          val udfImplementations = udfs.map({ case (_, impl) => impl })

          // Check no overlap between parameter and source names (they share the same namespace)
          val parametersSourcesIntersection =
            sources.keySet.intersect(parameters.keySet)
          if (parametersSourcesIntersection.nonEmpty) {
            c.abort(
              c.enclosingPosition,
              s"Parameters and Sources classes cannot have fields with the same names. Please remove/rename the following duplicates: ${parametersSourcesIntersection
                .mkString(", ")}")
          }

          val cacheLogger = (msg: String) => c.info(c.enclosingPosition, msg, false)

          val outputRecordFields =
            HiveCache
              .cached(hiveConf, sources, parameters, udfDescriptions, sqlStatement, cacheLogger)(
                schema => {
                  Option(schema)
                    .map(_.getFieldSchemas.asScala
                      .map(fieldSchema => {
                        val fieldName = fieldSchema.getName
                        val fieldType =
                          HiveType
                            .parseHiveType(fieldSchema.getType)
                            .fold(
                              missingType =>
                                c.abort(
                                  c.enclosingPosition,
                                  s"Could not find Scala type to match Hive type ${missingType} (in ${fieldSchema.getType}) for column ${fieldName}"),
                              identity)
                        (fieldName, fieldType)
                      })
                      .toList)
                    .getOrElse(
                      c.abort(c.enclosingPosition, s"Could not evaluate Hive Schema for ${tpname}")
                    )
                })
              .fold(
                ex =>
                  c.abort(
                    c.enclosingPosition,
                    s"Error compiling Hive Query for ${tpname}: ${ex}\n${ExceptionString(ex)}"),
                identity
              )

          // Find all the structs to generate appropriate types.
          val structs = outputRecordFields
            .flatMap({ case (_, hiveType)      => hiveType.allTypes })
            .collect({ case struct: StructType => struct })

          // The schema represents wildcard types using "<source>.<field>" whilst these fields
          // will be outputted in Parquet as just "<field>" so the <source> part needs to be
          // dropped.
          val fieldsToGenerate = outputRecordFields.map({
            case (fieldName, typ) => fieldName.split("\\.").last -> typ
          })

          // Check to see that we don't have any duplicate column names.
          val namesToOriginalSources = outputRecordFields
            .map({
              case (fieldName, typ) => fieldName.split("\\.").last -> fieldName
            })
            .groupBy(_._1)
          if (namesToOriginalSources.values.exists(_.size > 1)) {
            val prettyNames = namesToOriginalSources.values
              .filter(_.size > 1)
              .flatMap(_.map({
                case (name, source) => s"- ${name} is mapped to ${source}"
              }))
              .mkString("\n")
            c.abort(c.enclosingPosition,
                    s"Hive Query for ${tpname} would result in duplicate names:\n${prettyNames}")
          }

          // Make the returned row itself a struct for simplicity/elegance.
          val outputRecord = StructType(ListMap(fieldsToGenerate: _*))

          // Compose all of the structs together.
          val allStructs = (outputRecord +: structs).zipWithIndex.toMap

          // Start generating the scrooge objects.
          val scroogeGenerator = new ScroogeGenerator[c.type](c)

          // Resolve the type of each Hive Type to a concrete Scala Type
          def structName(struct: StructType): TypeName = {
            val idx = allStructs(struct)
            if (idx == 0)
              TypeName(s"Row") // Special case - first struct is the actual output record.
            else TypeName(s"Struct${idx}")
          }

          // Generate structs
          val generatedStructs = allStructs.toList.flatMap({
            case (struct, idx) => {
              scroogeGenerator.generateStruct(s => structName(s), struct)
            }
          })

          def readParametersAsMap(fields: Iterable[String]) = {
            val literals = fields.toList.map(parameterName => {
              q"${Literal(Constant(parameterName))} -> com.rouesnel.typedsql.SqlParameter.write(${TermName(parameterName)})"
            })
            q"Map(..${literals})"
          }

          def readSourcesAsMap(fields: Iterable[String]) = {
            val literals = fields.toList.map(parameterName => {
              q"${Literal(Constant(parameterName))} -> ${TermName(parameterName)}"
            })
            q"Map(..${literals})"
          }

          def createUdfMap() = {
            val literals = udfDescriptions.map(udf => {
              q"(${Literal(Constant(udf.name))}, ${Literal(Constant(udf.id))}) -> classOf[${TypeName("UDF_" + udf.name)}].asInstanceOf[Class[org.apache.hadoop.hive.ql.udf.generic.GenericUDF]]"
            })
            q"Map(..${literals})"
          }

          val amendedParents = parents :+ tq"com.rouesnel.typedsql.CompiledSqlQuery"
          q"""$mods object $tpname extends ..$amendedParents {
            ..$remainingMembers

            ..${generatedStructs}

            ..${udfImplementations}

            def sql: String = ${Literal(Constant(sqlStatement))}

            def query(...${queryParams}): com.rouesnel.typedsql.HiveQueryDataSource[Row] = {
             implicit def hasStructType: com.rouesnel.typedsql.core.HasStructType[Row] =
               com.rouesnel.typedsql.core.HasStructType[Row](com.rouesnel.typedsql.core.HiveType.parseHiveType(${Literal(
            Constant(outputRecord.hiveType))}).toOption.get.asInstanceOf[com.rouesnel.typedsql.core.StructType])
             com.rouesnel.typedsql.HiveQueryDataSource[Row](
               sql,
               ${readParametersAsMap(parameters.keys)},
               ${readSourcesAsMap(sources.keys)},
               ${createUdfMap()},
               this.partitions
              )
            }
          }"""

        }
        case _ =>
          c.abort(c.enclosingPosition, "Annotation @SqlQuery can only apply to objects")
      }
    }

    // Output the generated code to the target/typedsql directory.
    PersistGeneratedMacroCode[c.type](c)(result.toString)

    c.Expr[Any](result)
  }
}
