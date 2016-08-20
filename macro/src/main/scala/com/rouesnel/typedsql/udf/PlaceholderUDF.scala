package com.rouesnel.typedsql.udf

import argonaut._, Argonaut._

import com.rouesnel.typedsql.core._

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

import scalaz._, Scalaz._

object UdfDescription {

  implicit def hiveTypeCodec: CodecJson[HiveType] =
    codec1(
      (str: String) =>
        HiveType
          .parseHiveType(str)
          .fold(err => throw new Exception(s"Could not parse: ${str} (${err})"), identity),
      (ht: HiveType) => ht.hiveType
    )("hive_type")

  implicit def jsonCodec =
    casecodec3(UdfDescription.apply _, UdfDescription.unapply _)(
      "name",
      "arguments",
      "return_type"
    )
}

case class UdfDescription(
    name: String,
    arguments: List[(String, HiveType)],
    returnType: HiveType
)

abstract class PlaceholderUDF(val placeholderIndex: Int) extends GenericUDF {
  val description: UdfDescription = {
    PlaceholderUDF.udfs
      .get(placeholderIndex)
      .getOrElse(
        throw new Exception(s"Could not find UDF placeholder for index ${placeholderIndex}"))
  }

  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    arguments.toList
      .zip(description.arguments)
      .zipWithIndex
      .foreach({
        case ((argumentType, (_, expectedType)), idx) =>
          if (argumentType.getTypeName == expectedType.hiveType) {} else {
            throw new UDFArgumentTypeException(
              idx,
              s"Wrong type given to ${getUdfName()} - expected ${expectedType.hiveType} but instead got ${argumentType.getTypeName}.")
          }
      })

    description.returnType match {
      case pt: PrimitiveType =>
        pt match {
          case IntType =>
            PrimitiveObjectInspectorFactory.javaIntObjectInspector
          case StringType =>
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        }
      case other =>
        throw new UDFArgumentTypeException(0, s"Unsupported return type: ${other.hiveType}")
    }
  }

  override def getDisplayString(children: Array[String]): String =
    s"Placeholder for ${description.name}"

  override def getUdfName(): String = description.name

  override def evaluate(parameters: Array[DeferredObject]): AnyRef = {
    description.returnType match {
      case pt: PrimitiveType =>
        pt match {
          case IntType    => 0: java.lang.Integer
          case StringType => ""
        }
      case other =>
        throw new UDFArgumentTypeException(0, s"Unsupported return type: ${other.hiveType}")
    }
  }
}

object PlaceholderUDF {
  val numberOfPlaceholders = 15
  val udfs                 = collection.mutable.HashMap[Int, UdfDescription]()
  val placeholders = 0
    .to(numberOfPlaceholders)
    .toList
    .map(idx =>
      (udf: UdfDescription) => {
        udfs.put(idx, udf)
        getClass.getClassLoader
          .loadClass(s"com.rouesnel.typedsql.udf.PlaceholderUDF${idx}")
          .asInstanceOf[Class[PlaceholderUDF]]
    })

  def configurePlaceholders[T](udfs: List[UdfDescription])(
      f: (UdfDescription, Class[PlaceholderUDF]) => T): String \/ List[T] = {
    if (udfs.size > numberOfPlaceholders) {
      val prettyUdfs = udfs.map(udf => s"- ${udf.name}").mkString("\n")
      \/.left(
        s"Only ${numberOfPlaceholders} UDFs are supported and ${udfs.size} were given:\n${prettyUdfs}")
    } else {
      \/.right(
        udfs
          .zip(placeholders)
          .map({
            case (udf, getPlaceholderClass) => {
              val placeholderClass = getPlaceholderClass(udf)
              f(udf, placeholderClass)
            }
          }))
    }
  }
}
