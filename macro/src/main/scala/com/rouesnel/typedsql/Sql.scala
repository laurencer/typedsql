package com.rouesnel.typedsql

import scala.annotation.compileTimeOnly
import scala.annotation.StaticAnnotation
import scala.collection.convert.decorateAsScala._
import scala.collection.immutable.ListMap
import scala.concurrent._, duration._
import scala.language.experimental.macros
import scala.reflect.macros._

import scalaz._, Scalaz._

import com.rouesnel.typedsql.core._

@compileTimeOnly("enable macro paradise to expand macro annotations")
class SqlQuery extends StaticAnnotation {
  def macroTransform(annottees: Any*) = macro SqlQuery.impl
}

object SqlQuery {
  import au.com.cba.omnia.beeswax.Hive
  import org.apache.hadoop.hive.ql.Driver
  import org.apache.hadoop.hive.ql.session.SessionState

  val futureHiveConf = HiveSupport.initialize()
  def hiveConf = Await.result(futureHiveConf, 25.seconds)

  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    val sourceMapping = new SourceMapping(c)
    val parameterMapping = new ParameterMapping(c)

    val result = {
      annottees.map(_.tree).toList match {
        case q"$mods object $tpname extends ..$parents { ..$stats }" :: Nil => {
          val sqlLiteral = stats
            .collect({ case q"def query = $sqlQuery" => sqlQuery })
            .headOption
            .getOrElse(c.abort(c.enclosingPosition, "Must have a function called `query`."))
            match {
              case Literal(Constant(code)) => code
              case _ => c.abort(c.enclosingPosition, "Query function must be a literal string.")
          }

          val sqlStatement = sqlLiteral.toString.trim.replaceAll("\\n", " ")
          if (sqlStatement.split(";").length != 1) {
            c.abort(c.enclosingPosition, s"Only a single SQL statement is supported. Please remove any ';'s from $sqlLiteral")
          }

          // Get sources and create temp tables mapping to them.
          val sources = sourceMapping.readSourcesClass(stats)

          // Get the parameters.
          val parameters = parameterMapping.readParametersClass(stats)

          // Check no overlap between parameter and source names (they share the same namespace)
          val parametersSourcesIntersection = sources.keySet.intersect(parameters.keySet)
          if (parametersSourcesIntersection.nonEmpty) {
            c.abort(c.enclosingPosition, s"Parameters and Sources classes cannot have fields with the same names. Please remove/rename the following duplicates: ${parametersSourcesIntersection.mkString(", ")}")
          }

          // Retrieve the schema from the compiled query.
          /*
          val schema = HiveQuery.compileQuery(hiveConf, sources, parameters, sqlStatement)
            .fold(ex => c.abort(c.enclosingPosition, ex.toString), identity)

          // Parse the types from the schema.
          val outputRecordFields = schema.getFieldSchemas.asScala.map(fieldSchema => {
            val fieldName = fieldSchema.getName
            val fieldType =
              HiveType.parseHiveType(fieldSchema.getType)
                  .fold(missingType => c.abort(c.enclosingPosition, s"Could not find Scala type to match Hive type ${missingType} (in ${fieldSchema.getType}) for column ${fieldName}"), identity)
            (fieldName, fieldType)
          }).toList

          */

          val outputRecordFields = HiveCache.cached(hiveConf, sources, parameters, sqlStatement)(schema => {
            schema.getFieldSchemas.asScala.map(fieldSchema => {
              val fieldName = fieldSchema.getName
              val fieldType =
                HiveType.parseHiveType(fieldSchema.getType)
                  .fold(missingType => c.abort(c.enclosingPosition, s"Could not find Scala type to match Hive type ${missingType} (in ${fieldSchema.getType}) for column ${fieldName}"), identity)
              (fieldName, fieldType)
            }).toList
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

          // Find all the structs to generate appropriate types.
          val structs = outputRecordFields
            .flatMap({ case (_, hiveType) => hiveType.allTypes })
            .toList
            .collect({ case struct: StructType => struct})

          // Map the nested structs to the generated ones and remove any nested fields
          val fieldsToGenerate = outputRecordFields
            .filter({ case (fieldName, _) => !fieldName.contains(".") }) ++
            implicitStructs.toList.map({ case (fieldNameParts, fieldStruct) =>
              fieldNameParts.mkString(".") -> fieldStruct
            })

          // Make the returned row itself a struct for simplicity/elegance.
          val outputRecord = StructType(ListMap(fieldsToGenerate: _*))

          // Compose all of the structs together.
          val allStructs = (outputRecord +: (implicitStructs.values.toList ++ structs))
            .zipWithIndex
            .toMap

          // Start generating the scrooge objects.
          val scroogeGenerator = new ScroogeGenerator[c.type](c)

          // Resolve the type of each Hive Type to a concrete Scala Type
          def structName(struct: StructType): TypeName = {
            val idx = allStructs(struct)
            if (idx == 0) TypeName(s"Row") // Special case - first struct is the actual output record.
            else TypeName(s"Struct${idx}")
          }

          // Generate structs
          val generatedStructs = allStructs.toList.flatMap({ case (struct, idx) => {
            scroogeGenerator.generateStruct(s => structName(s), struct)
          }})

          def readParametersAsMap(sourcesObjectName: String, fields: Iterable[String]) = {
            val literals = fields.toList.map(parameterName => {
              q"${Literal(Constant(parameterName))} -> com.rouesnel.typedsql.SqlParameter.write(${TermName(sourcesObjectName)}.${TermName(parameterName)})"
            })
            q"Map(..${literals})"
          }

          def readSourcesAsMap(sourcesObjectName: String, fields: Iterable[String]) = {
            val literals = fields.toList.map(parameterName => {
              q"${Literal(Constant(parameterName))} -> ${TermName(sourcesObjectName)}.${TermName(parameterName)}"
            })
            q"Map(..${literals})"
          }

          val amendedParents = parents :+ tq"com.rouesnel.typedsql.CompiledSqlQuery"
          q"""$mods object $tpname extends ..$amendedParents {
            ..$stats

            ..${generatedStructs}

            def apply(srcs: Sources, params: Parameters): com.rouesnel.typedsql.DataSource[Row] = {
             com.rouesnel.typedsql.HiveQueryDataSource[Row](query, ${readParametersAsMap("params", parameters.keys)}, ${readSourcesAsMap("srcs", sources.keys)})
            }
          }"""

        }
        case _ => c.abort(c.enclosingPosition, "Annotation @SqlQuery can only apply to objects")
      }
    }
    c.Expr[Any](result)
  }
}
