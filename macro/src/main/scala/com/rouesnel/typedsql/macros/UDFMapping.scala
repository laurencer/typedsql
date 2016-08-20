package com.rouesnel.typedsql.macros

import com.rouesnel.typedsql.core._
import com.rouesnel.typedsql.udf
import com.rouesnel.typedsql.udf._

import scala.reflect.api.Trees
import scala.reflect.macros.whitebox
import scala.util.Random
import scala.util.hashing.MurmurHash3

class UDFMapping[C <: whitebox.Context](val c: C) {
  import c.universe._

  val hiveTypeMapping = new ThriftHiveTypeMacro[c.type](c)

  def toHiveType(tpt: Type): HiveType = {
    if (tpt == null) {
      c.abort(c.enclosingPosition, "tpt is null")
    }

    hiveTypeMapping.convertScalaToHiveType(tpt)
  }

  private def hiveTypeToUdfInspector(typ: HiveType): Tree = {
    val primitiveObjectInspectorFactory =
      q"org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory"
    val mapping = typ match {
      case BooleanType => "Boolean"
      case TinyIntType => "Byte"
      case ShortType   => "Short"
      case IntType     => "Int"
      case LongType    => "Long"
      case StringType  => "String"
      case FloatType   => "Float"
      case DoubleType  => "Double"
      case DateType    => "Date"
      // case DecimalType(_, _) => "HiveDecimal"
      case other =>
        c.abort(c.enclosingPosition, s"${other} is not a support type for UDF parameters.")
    }
    q"${primitiveObjectInspectorFactory}.${TermName("java" + mapping + "ObjectInspector")}"
  }

  private def primitiveMatchers(typ: HiveType): List[CaseDef] = typ match {
    case BooleanType =>
      List(
        cq"iw: org.apache.hadoop.io.BooleanWritable => iw.get()"
      )
    case TinyIntType =>
      List(
        cq"iw: org.apache.hadoop.io.ByteWritable => iw.get()",
        cq"iw: org.apache.hadoop.hive.serde2.io.ByteWritable => iw.get()"
      )
    case ShortType =>
      List(
        cq"iw: org.apache.hadoop.io.ShortWritable => iw.get()",
        cq"iw: org.apache.hadoop.hive.serde2.io.ShortWritable => iw.get()"
      )
    case IntType =>
      List(
        cq"iw: org.apache.hadoop.io.IntWritable => iw.get()"
      )
    case LongType =>
      List(
        cq"iw: org.apache.hadoop.io.IntWritable => iw.get()",
        cq"iw: org.apache.hadoop.io.LongWritable => iw.get()"
      )
    case FloatType =>
      List(
        cq"f: java.lang.Float => f",
        cq"iw: org.apache.hadoop.io.FloatWritable => iw.get()"
      )
    case DoubleType =>
      List(
        cq"iw: org.apache.hadoop.io.FloatWritable  => iw.get()",
        cq"iw: org.apache.hadoop.io.DoubleWritable => iw.get()",
        cq"iw: org.apache.hadoop.hive.serde2.io.DoubleWritable => iw.get()"
      )
    case DateType =>
      List(
        cq"date: java.sql.Date => date",
        cq"iw: org.apache.hadoop.hive.serde2.io.DateWritable => iw.get()"
      )
    case StringType =>
      List(
        cq"str: AnyRef => str.toString"
      )
    case other =>
      c.abort(c.enclosingPosition, s"${other.hiveType} is not yet supported for UDF parameters.")
  }

  private def generateUdf(udf: UdfDescription, body: Tree): Tree = {

    val parameterAccessors = udf.arguments.zipWithIndex.map({
      case ((name, typ), idx) =>
        q"""val ${TermName(name)} = (_parameters(${Literal(Constant(idx))}).get().asInstanceOf[AnyRef] match {
              case ..${primitiveMatchers(typ)}
            }).asInstanceOf[${hiveTypeMapping.convertHiveTypeToScalaType(typ)}]
         """
    })

    val generatedUdf = q"""
      class ${TypeName("UDF_" + udf.name)} extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF {

       import org.apache.hadoop.hive.ql.exec._
       import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
       import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
       import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
       import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

       override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
        if (arguments.length != ${Literal(Constant(udf.arguments.size))}) {
         throw new UDFArgumentLengthException(${Literal(Constant(udf.name))} + " only takes " + ${Literal(
      Constant(udf.arguments.size))}.toString() + " arguments.")
        }

        ${hiveTypeToUdfInspector(udf.returnType)}
       }

       override def getDisplayString(children: Array[String]): String =
         ${Literal(Constant(udf.name))}

       override def evaluate(_parameters: Array[DeferredObject]): AnyRef = {
         ..$parameterAccessors

         ($body).asInstanceOf[${hiveTypeMapping.convertHiveTypeToScalaType(udf.returnType)}].asInstanceOf[AnyRef]
       }
     }
    """

    generatedUdf
  }

  def readUDFs(enclosingObjectName: String,
               objectBody: Seq[Trees#Tree]): List[(UdfDescription, Tree)] = {
    objectBody
      .collect({
        case q"$mods def $tname(..${ paramss }): $tpt = $expr" =>
          (mods, tname, paramss, tpt, expr)
      })
      .toList
      .flatMap({
        case (mods, tname, paramss, tpt, expr) => {
          val Modifiers(_, _, annotations) = mods
          annotations.collect({
            case q"new ${ typ }"
                if c
                  .typecheck(typ, c.TYPEmode)
                  .tpe =:= c.typecheck(tq"com.rouesnel.typedsql.UDF", c.TYPEmode).tpe => {
              (tname, paramss, tpt)

              val description = udf.UdfDescription(
                s"${tname.toString()}_${MurmurHash3.stringHash(enclosingObjectName).toString.replace('-', '0')}",
                tname.toString(),
                paramss
                  .map({
                    case q"$mods val $name: $pTpt = $body" => {
                      val resolvedTpe =
                        Option(c.typecheck(pTpt, mode = c.TYPEmode).tpe)
                      name.toString -> resolvedTpe
                        .map(toHiveType(_))
                        .getOrElse(c.abort(
                          c.enclosingPosition,
                          s"Could not resolve type for parameter ${name} ($pTpt) in UDF ${tname}"))
                    }
                  })
                  .toList, {
                  val resolvedTpe =
                    Option(c.typecheck(tpt, mode = c.TYPEmode).tpe)
                  resolvedTpe
                    .map(toHiveType(_))
                    .getOrElse(c.abort(c.enclosingPosition,
                                       s"Could not resolve return type ($tpt) in UDF ${tname}"))
                }
              )

              val generatedUdf = generateUdf(description, expr)

              (description, generatedUdf)
            }
          })
        }
      })
  }
}
