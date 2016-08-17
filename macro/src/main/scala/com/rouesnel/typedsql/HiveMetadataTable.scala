package com.rouesnel.typedsql

import au.com.cba.omnia.beeswax.{Hive, ParquetFormat}
import com.rouesnel.typedsql.core.StructType

import scala.collection.JavaConversions._
import scala.collection.{Map => CMap}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.{AlreadyExistsException, FieldSchema, StorageDescriptor, Table => MetadataTable}
import org.apache.thrift.protocol.TType
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec, ThriftStructField}

import scala.util.control.NonFatal

import scalaz._, Scalaz._

/** Replicates the functionality from cascading-hive.*/
object HiveMetadataTable {

  /** While the earlier operations failed with exception from with the cascading-hive code,
   * we need to deal with failure via `Result`
   */
  def apply[T <: ThriftStruct](
                                database: String,
                                tableName: String,
                                partitionColumns: List[(String, String)],
                                location: Option[Path] = None
                              )(implicit m: Manifest[T]): MetadataTable = { // This operation could fail so type should convey it
    val metadata              = ThriftHiveType.apply[T].asInstanceOf[StructType]

    //Making the types lowercase to enforce the same behaviour as in cascading-hive
    val columnFieldSchemas    = metadata.fields.toList.map { c =>
      fieldSchema(c._1, c._2.hiveType)
    }

    val partitionFieldSchemas = partitionColumns.map {case(n, t) => fieldSchema(n, t)}

    assert(
      partitionColumns.map(_._1).toSet.intersect(metadata.fields.map(_._1).toSet).isEmpty,
      "Partition columns must be different from the fields in the thrift struct"
    )

    val table = new MetadataTable();
    table.setDbName(database.toLowerCase);
    table.setTableName(tableName.toLowerCase)

    val sd = new StorageDescriptor();
    columnFieldSchemas.foreach(f => sd.addToCols(f))

    location.fold(table.setTableType(TableType.MANAGED_TABLE.toString()))(p => {
      table.setTableType(TableType.EXTERNAL_TABLE.toString())
      //Copied from cascading-hive - Need to set this as well since setting the table type would be too obvious
      table.putToParameters("EXTERNAL", "TRUE");
      sd.setLocation(p.toString())
    })
    table.setSd(sd)

    if (!partitionFieldSchemas.isEmpty) {
      table.setPartitionKeys(partitionFieldSchemas)
      table.setPartitionKeysIsSet(true)
    }
    ParquetFormat.applyFormat(table)
  }

  def fieldSchema(n: String, t: String) =
    new FieldSchema(n, t, "Created by Ebenezer")


  def createTable[T <: ThriftStruct : Manifest](database: String, table: String, partitionColumns: List[(String, String)],
                  location: Option[Path] = None): Hive[Boolean] = {
      Hive.createDatabase(database)     >>
      Hive.existsTable(database, table) >>= (exists =>
      if (exists)
        Hive.mandatory(
          Hive.existsTableStrict[T](database, table, partitionColumns, location, ParquetFormat),
          s"$database.$table already exists but has different schema."
        ).map(_ => false)
      else
        Hive.getConfClient >>= { case (conf, client) =>
          val fqLocation = location.map(FileSystem.get(conf).makeQualified(_))
          val metadataTable = HiveMetadataTable[T](database, table, partitionColumns, fqLocation)

          try {
            client.createTable(metadataTable)
            Hive.value(true)
          } catch {
            case _: AlreadyExistsException =>
              Hive.mandatory(
                Hive.existsTableStrict[T](database, table, partitionColumns, location, ParquetFormat),
                s"$database.$table already exists but has different schema."
              ).map(_ => false)
            case NonFatal(t)               => Hive.error(s"Failed to create table $database.$table", t)
          }
        }
      )
  }
}
