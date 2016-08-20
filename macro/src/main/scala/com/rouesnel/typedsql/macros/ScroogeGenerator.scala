package com.rouesnel.typedsql.macros

import com.rouesnel.typedsql.core._

import scala.reflect.macros.whitebox

class ScroogeGenerator[Context <: whitebox.Context](val c: Context) {
  import c.universe._

  /** From a Hive Struct generate a Scrooge compatible/similar case class and object */
  def generateStruct(structNamer: StructType => TypeName, struct: StructType): List[c.Tree] = {
    val generatedName = structNamer(struct)

    // Get all of the fields in the struct.
    val fields = struct.fields.toList.map({
      case (fieldName, fieldType) =>
        q"${TermName(HiveType.camelCaseFieldName(fieldName))}: ${resolveType(structNamer, fieldType)}"
    })

    // Generate the {name}Field and {name}FieldManifest fields.
    val thriftFields = struct.fields.toList.zipWithIndex.flatMap({
      case ((fieldName, fieldType), idx) =>
        List(q"""
              val ${TermName(HiveType.camelCaseFieldName(fieldName).capitalize + "Field")} = new org.apache.thrift.protocol.TField(${Literal(
               Constant(fieldName))}, ${hiveTypeToThriftTypeName(fieldType)}, ${Literal(
               Constant(idx + 1))})""",
             q"""val ${TermName(
               HiveType
                 .camelCaseFieldName(fieldName)
                 .capitalize + "FieldManifest")} = implicitly[Manifest[${resolveType(
               structNamer,
               fieldType)}]]""")
    })

    // Generate the case class for the struct as well as the ThriftStructCodec3 for Parquet
    // compatibility
    List(
      q"""
        case class ${generatedName}(..$fields) extends com.twitter.scrooge.ThriftStruct {
          override def write(oprot: org.apache.thrift.protocol.TProtocol): Unit = {
            throw new scala.NotImplementedError("TypedSQL does not support writing the generated structs (" + ${Literal(
        Constant(generatedName.toString))} + ")")
          }
        }
       """,
      q"""
       object ${generatedName.toTermName} extends com.twitter.scrooge.ThriftStructCodec3[${generatedName}] {
         ..${thriftFields}
         ..${ttypeToHumanMethod}
         override def encode(t: ${generatedName}, oprot: org.apache.thrift.protocol.TProtocol): Unit = {
           throw new scala.NotImplementedError("TypedSQL does not support encoding fields in generated structs (" + ${Literal(
        Constant(generatedName.toString))} + ")")
         }
         ..${buildThriftDecode(structNamer, generatedName, struct.camelCasedFields.toList)}
       }
      """
    )
  }

  /** The ttypeToHumanMethod required for pretty printing errors/incorrect field types */
  lazy val ttypeToHumanMethod = q"""
   private def ttypeToHuman(byte: Byte) = {
    // from https://github.com/apache/thrift/blob/master/lib/java/src/org/apache/thrift/protocol/TType.java
      import org.apache.thrift.protocol.TType
      byte match {
        case TType.STOP   => "STOP"
        case TType.VOID   => "VOID"
        case TType.BOOL   => "BOOL"
        case TType.BYTE   => "BYTE"
        case TType.DOUBLE => "DOUBLE"
        case TType.I16    => "I16"
        case TType.I32    => "I32"
        case TType.I64    => "I64"
        case TType.STRING => "STRING"
        case TType.STRUCT => "STRUCT"
        case TType.MAP    => "MAP"
        case TType.SET    => "SET"
        case TType.LIST   => "LIST"
        case TType.ENUM   => "ENUM"
        case _            => "UNKNOWN"
      }
    }
  """

  /**
    * Resolves a HiveType to a specific Scala type.
    *
    * Slightly complicated that we need to be able to resolve Hive structs to specific Scala
    * types that have been generated (or will be generated shortly).
    */
  def resolveType(structNamer: StructType => TypeName, fieldType: HiveType): Tree =
    fieldType match {
      case p: PrimitiveType =>
        p match {
          case TinyIntType       => tq"Byte"
          case ShortType         => tq"Short"
          case IntType           => tq"Int"
          case LongType          => tq"Long"
          case FloatType         => tq"Float"
          case DoubleType        => tq"Double"
          case StringType        => tq"String"
          case BooleanType       => tq"Boolean"
          case DateType          => tq"java.util.Date"
          case DecimalType(_, _) => tq"java.math.BigDecimal"
        }
      case ArrayType(valueType) =>
        tq"List[${resolveType(structNamer, valueType)}]"
      case MapType(keyType, valueType) =>
        tq"Map[${resolveType(structNamer, keyType)}, ${resolveType(structNamer, valueType)}]"
      case s: StructType => tq"${structNamer(s)}"
    }

  /**
    * Maps a Hive type to the Thrift protocol types.
    */
  def hiveTypeToThriftTypeName(typ: HiveType): Tree = typ match {
    case p: PrimitiveType =>
      q"org.apache.thrift.protocol.TType.${TermName(p.thriftTypeName)}"
    case m: MapType => q"org.apache.thrift.protocol.TType.${TermName("MAP")}"
    case l: ArrayType =>
      q"org.apache.thrift.protocol.TType.${TermName("LIST")}"
    case s: StructType =>
      q"org.apache.thrift.protocol.TType.${TermName("STRUCT")}"
  }

  /**
    * Generates the map decode method for any map fields.
    */
  def buildMapDecode(keyType: Tree, valueType: Tree, readKey: Tree, readValue: Tree) = {
    q"""
         val _map = _iprot.readMapBegin()
         if (_map.size == 0) {
           _iprot.readMapEnd()
           Map.empty[${keyType}, ${valueType}]
         } else {
           val _rv = new scala.collection.mutable.HashMap[${keyType}, ${valueType}]
           var _i = 0
           while (_i < _map.size) {
             val _key = ${readKey}
             val _value = ${readValue}
             _rv(_key) = _value
             _i += 1
          }
          _iprot.readMapEnd()
          _rv.toMap
         }
      """
  }

  /**
    * Generates the array decode method for any array fields.
    */
  def buildArrayDecode(valueType: Tree, readValue: Tree) = {
    q"""
         val _list = _iprot.readListBegin()
         if (_list.size == 0) {
           _iprot.readListEnd()
           Nil
         } else {
           val _rv = new scala.collection.mutable.ArrayBuffer[${valueType}](_list.size)
           var _i = 0
           while (_i < _list.size) {
             _rv += {
                 ${readValue}
             }
             _i += 1
           }
           _iprot.readListEnd()
           _rv.toList
         }
      """
  }

  /** Generates the method for reading the particular field value. */
  def readerForType(structNamer: StructType => TypeName, hiveType: HiveType): Tree =
    hiveType match {
      case p: PrimitiveType =>
        q"_iprot.${TermName("read" + p.thriftTypeName.toLowerCase.capitalize)}"
      case m: MapType =>
        buildMapDecode(resolveType(structNamer, m.key),
                       resolveType(structNamer, m.value),
                       readerForType(structNamer, m.key),
                       readerForType(structNamer, m.value))
      case a: ArrayType =>
        buildArrayDecode(resolveType(structNamer, a.valueType),
                         readerForType(structNamer, a.valueType))
      case s: StructType => q"${structNamer(s).toTermName}.decode(_iprot)"
    }

  /**
    * Generates the decode method for a thrift struct.
    */
  def buildThriftDecode(structNamer: StructType => TypeName,
                        structName: TypeName,
                        fields: List[(String, HiveType)]) = {
    def readMethodName(fieldName: String) =
      TermName("read" + fieldName.capitalize + "Value")

    // Methods for reading each field.
    val readMethods = fields.map({
      case (fieldName, hiveType) =>
        q"""private def ${readMethodName(fieldName)}(_iprot: org.apache.thrift.protocol.TProtocol): ${resolveType(
          structNamer,
          hiveType)} = {
            ${readerForType(structNamer, hiveType)}
          }
       """
    })

    // Mutable placeholder fields used to store each field as its read.
    val placeholderFields = fields.map({
      case (fieldName, hiveType) => {
        val placeholderValue = hiveType match {
          case p: PrimitiveType =>
            p match {
              case BooleanType       => q"false"
              case TinyIntType       => q"0.toByte"
              case ShortType         => q"0"
              case IntType           => q"0"
              case LongType          => q"0L"
              case FloatType         => q"0.0f"
              case DoubleType        => q"0.0"
              case DateType          => q"new java.util.Date()"
              case DecimalType(_, _) => q"new java.math.BigDecimal()"
              case StringType        => q""" "" """
            }
          case _ => q"null"
        }
        q"var ${TermName(fieldName)}: ${resolveType(structNamer, hiveType)} = ${placeholderValue}"
      }
    })

    // Extracts a given field into the mutable placeholder (or throws an error on invalid data)
    val fieldMatchers = fields.zipWithIndex.map({
      case ((fieldName, hiveType), idx) =>
        val fieldExtractor =
          q"""
           _field.`type` match {
            case ${hiveTypeToThriftTypeName(hiveType)} => {
              ${TermName(fieldName)} = ${readMethodName(fieldName)}(_iprot)
            }
           case _actualType => {
             val _expectedType = ${hiveTypeToThriftTypeName(hiveType)}
             val errorMessage = ("Received wrong type for field " + ${Literal(Constant(fieldName))} + " (expected=%s, actual=%s).").format(
                                   ttypeToHuman(_expectedType),
                                   ttypeToHuman(_actualType)
                                 )
             throw new org.apache.thrift.protocol.TProtocolException(errorMessage)
           }
         }
         """
        cq"""${Literal(Constant(idx + 1))} => { $fieldExtractor }"""
    })

    // Builds the case class from the temporary/placeholder fields.
    def classConstructor = {
      val fieldValues = fields.map({
        case (fieldName, hiveType) => {
          q"${TermName(fieldName)}"
        }
      })
      q"${structName.toTermName}(..$fieldValues)"
    }

    // Build the actual decode method.
    val decode =
      q"""
        override def decode(_iprot: org.apache.thrift.protocol.TProtocol): ${structName} = {
          ..${placeholderFields}
          var _done = false
          _iprot.readStructBegin()
          while(!_done) {
            val _field = _iprot.readFieldBegin()
            if (_field.`type` == org.apache.thrift.protocol.TType.STOP) {
              _done = true
            } else {
              _field.id match {
                case ..$fieldMatchers
              }
              _iprot.readFieldEnd()
            }
          }
          _iprot.readStructEnd()

          ${classConstructor}
        }
       """

    readMethods :+ decode
  }
}
