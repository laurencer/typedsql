package com.rouesnel.typedsql

import com.rouesnel.typedsql.core._

import com.twitter.scrooge.ThriftStruct

import scala.reflect.macros._

import scala.collection.immutable.ListMap


object GenerateHiveType {
  def apply[T] = macro impl[T]

  def impl[T](c: Context)(implicit typ: c.WeakTypeTag[T]): c.Expr[HiveType] = {
    import c.universe._

    val hiveType = new ThriftHiveTypeMacro[c.type](c)
      .convertScalaToHiveType(c.weakTypeOf[T])

    c.Expr[HiveType](
      q"com.rouesnel.typedsql.core.HiveType.parseHiveType(${Literal(Constant(hiveType.hiveType))}).toOption.get"
    )
  }
}

object ThriftStructType {
  def apply[T <: ThriftStruct] = macro impl[T]

  def impl[T](c: Context)(implicit typ: c.WeakTypeTag[T]): c.Expr[StructType] = {
    import c.universe._

    val hiveType = new ThriftHiveTypeMacro[c.type](c)
      .convertScalaToHiveType(c.weakTypeOf[T])

    c.Expr[StructType](
      q"com.rouesnel.typedsql.core.HiveType.parseHiveType(${Literal(Constant(hiveType.hiveType))}).toOption.get.asInstanceOf[com.rouesnel.typedsql.core.StructType]"
    )
  }
}

class ThriftHiveTypeMacro[C <: Context](val c: C) {
  import c.universe._
  def listMap[K, V](els: Seq[(K, V)]): ListMap[K, V] = ListMap(els: _*)

  val seqType       = c.weakTypeOf[Seq[_]]
  val mapType       = c.weakTypeOf[scala.collection.Map[_, _]]
  val intType       = c.weakTypeOf[Int]
  val longType      = c.weakTypeOf[scala.Long]
  val javaLongType  = c.weakTypeOf[java.lang.Long]
  val booleanType   = c.weakTypeOf[Boolean]
  val doubleType    = c.weakTypeOf[Double]
  val stringType    = c.weakTypeOf[String]
  val dateType      = c.weakTypeOf[java.util.Date]
  val shortType     = c.weakTypeOf[Short]
  val byteType      = c.weakTypeOf[Byte]
  val floatType     = c.weakTypeOf[Float]
  val bigDecimalType = c.weakTypeOf[java.math.BigDecimal]
  val thriftType     = c.weakTypeOf[ThriftStruct]

  /** Converts a Scala type to a Hive type */
  def convertScalaToHiveType(tpe: Type): HiveType = tpe match {
    case typ if (typ <:< doubleType)       => DoubleType
    case typ if (typ <:< floatType)        => FloatType
    case typ if (typ <:< longType)         => LongType
    case typ if (typ <:< javaLongType)     => LongType
    case typ if (typ <:< intType)          => IntType
    case typ if (typ <:< shortType)        => ShortType
    case typ if (typ <:< byteType)         => TinyIntType
    case typ if (typ <:< booleanType)      => BooleanType
    case typ if (typ <:< dateType)         => DateType
    case typ if (typ <:< bigDecimalType)   => DecimalType(10, 0)
    case typ if (typ <:< stringType)       => StringType
    case map if (map <:< mapType) => {
      val key :: value :: Nil = map.typeArgs
      MapType(convertScalaToHiveType(key), convertScalaToHiveType(value))
    }
    case seq if (seq <:< seqType) => {
      val inner = seq.typeArgs.head
      ArrayType(convertScalaToHiveType(inner))
    }
    case struct if (struct <:< thriftType) => {
      mapObjectTypeToHiveSchema(struct.companion)
    }
    case other => {
      c.abort(c.enclosingPosition, s"Could not convert scala type ${other} (${showRaw(other)}) to a Hive Type.")
    }
  }

  def convertHiveTypeToScalaType(hiveType: HiveType): c.Type = hiveType match {
    case BooleanType  => booleanType
    case DoubleType   => doubleType
    case IntType      => intType
    case FloatType    => floatType
    case LongType     => longType
    case ShortType    => shortType
    case TinyIntType  => byteType
    case DateType     => dateType
    case StringType   => stringType
    case _: DecimalType => bigDecimalType
    case MapType(key, value) => tq"scala.collection.Map[${convertHiveTypeToScalaType(key)}, ${convertHiveTypeToScalaType(value)}]".tpe
    case ArrayType(value)    => tq"scala.collection.Seq[${convertHiveTypeToScalaType(value)}]".tpe
    case StructType(_) => c.abort(c.enclosingPosition, "Struct/complex types are not supported.")
  }

  /** Converts a Scrooge struct type to a Hive type */
  def mapObjectTypeToHiveSchema(thriftCompanion: Type): StructType = {
    // Now we can extract all relevant fields and reverse a schema.
    // 1. Extract the val {name}Field = new TField(...) methods
    val fieldNamesFromCompanion = thriftCompanion.members.toList.collect({
      case m: MethodSymbol if m.name.toString.endsWith("Field") && (! m.isPrivate) => {
        val fieldName = m.name.toString
        fieldName.substring(0, fieldName.length - "Field".length)
      }
    })

    // 2. Extract the reader fields to work out each return type.
    val readerMethodNames = fieldNamesFromCompanion.map(fieldName => {
      s"read${fieldName}Value" -> fieldName
    }).toMap
    val readerFields = thriftCompanion.members.toList.collect({
      case m: MethodSymbol if readerMethodNames.contains(m.name.toString) => {
        readerMethodNames(m.name.toString) -> m.returnType
      }
    })

    // 3. Perform case conversion.
    // Convert capitals to underscores unless followed by multipled capitals.
    val cleanedFields = readerFields.map({ case (name, fieldType) =>
      // Stolen from http://stackoverflow.com/a/1176023/49142
      val underscoredName =
        name
          .replaceAll("""(.)([A-Z][a-z]+)""", """$1_$2""")
          .replaceAll("""([a-z0-9])([A-Z])""", "$1_$2")
          .toLowerCase()

      underscoredName -> fieldType
    }).reverse


    StructType(listMap(cleanedFields.map({
      case (fieldName, fieldType) => fieldName -> convertScalaToHiveType(fieldType)
    })))
  }
}
