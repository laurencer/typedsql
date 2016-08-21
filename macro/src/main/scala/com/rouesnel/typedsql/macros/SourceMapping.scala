package com.rouesnel.typedsql.macros

import com.rouesnel.typedsql.DataSource
import com.rouesnel.typedsql.core._

import scala.reflect.api.Trees
import scala.reflect.macros.whitebox
import scalaz.{-\/, \/, \/-}

class SourceMapping[C <: whitebox.Context](val c: C) {
  import c.universe._

  val partitionMapping = new PartitionMapping[c.type](c)

  def dataSourceType = typeOf[DataSource[_, _]]

  def readSources(parameters: Seq[Trees#Tree])
    : (Seq[Trees#Tree], Map[String, (StructType, List[(String, HiveType)])]) = {

    val processed = parameters.collect({
      case q"$mods val ${ name }: $tpt" if isDataSource(tpt) => {
        val (objectType, partitionsType) = extractDataSource(tpt)

        // First resolve the object type.
        // val typeChecked = c.typecheck(objectType, c.TYPEmode)
        val tpe = Option(objectType.companion).getOrElse(
          c.abort(c.enclosingPosition, "Could not determine type of " + objectType.toString()))
        // Ensure its a ThriftStructCodec3
        if (!tpe.weak_<:<(
              c.typecheck(tq"com.twitter.scrooge.ThriftStructCodec3[_]", c.TYPEmode).tpe)) {
          c.abort(c.enclosingPosition, s"${objectType} must be a subtype of ThriftStructCodec3[_]")
        }

        val cleanedFields =
          new ThriftHiveTypeMacro[c.type](c).mapObjectTypeToHiveSchema(tpe)

        // Now resolve the parition fields.
        val partitionFields = partitionMapping.readPartitions(partitionsType)

        \/.right(name.toString -> (cleanedFields, partitionFields))
      }
      case other => \/.left(other)
    })

    processed.collect({ case -\/(skipped) => skipped }) -> processed
      .collect({ case \/-(source)         => source })
      .toMap
  }

  def isDataSource(typ: Tree): Boolean = c.typecheck(typ, c.TYPEmode).tpe <:< dataSourceType

  def extractDataSource(typ: Tree): (Type, Type) =
    c.typecheck(typ, c.TYPEmode).tpe.normalize match {
      case TypeRef(_, _, objectType :: partitionsType :: Nil) => {
        (objectType, partitionsType)
      }
      case TypeRef(_, _, other) => {
        c.abort(c.enclosingPosition,
                s"Got too many parameters for DataSource type (expected 2): ${typ}")
      }
      case other =>
        c.abort(c.enclosingPosition, s"Expected DataSource type but instead got: ${typ}")
    }
}
