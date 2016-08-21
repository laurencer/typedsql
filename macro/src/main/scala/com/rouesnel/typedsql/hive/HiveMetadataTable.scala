package com.rouesnel.typedsql.hive

import au.com.cba.omnia.beeswax._
import au.com.cba.omnia.omnitool.Result
import com.rouesnel.typedsql.core._
import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.{
  AlreadyExistsException,
  FieldSchema,
  NoSuchObjectException,
  StorageDescriptor,
  Table => MetadataTable
}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.convert.decorateAsScala._
import scala.collection.{Map => CMap}
import scala.util.control.NonFatal
import scalaz.Scalaz._
import scalaz._

/** Replicates the functionality from cascading-hive and beeswax s.*/
object HiveMetadataTable {
  val log = LoggerFactory.getLogger(getClass)

  /** While the earlier operations failed with exception from with the cascading-hive code,
    * we need to deal with failure via `Result`
    */
  def apply[T <: ThriftStruct: HasStructType, P: Partitions](database: String,
                                                             tableName: String,
                                                             location: Option[Path] = None)(
      implicit m: Manifest[T])
    : MetadataTable = { // This operation could fail so type should convey it

    val structType = implicitly[HasStructType[T]].structType
    // Making the types lowercase to enforce the same behaviour as in cascading-hive
    val columnFieldSchemas = structType.fields.toList.map { c =>
      fieldSchema(c._1, c._2.hiveType)
    }

    val partitionFieldSchemas = implicitly[Partitions[P]].fields.map {
      case (n, t) => fieldSchema(n, t.hiveType)
    }

    assert(
      implicitly[Partitions[P]].fields
        .map(_._1)
        .toSet
        .intersect(structType.fields.map(_._1).toSet)
        .isEmpty,
      "Partition columns must be different from the fields in the thrift struct"
    )

    val table = new MetadataTable()
    table.setDbName(database.toLowerCase)
    table.setTableName(tableName.toLowerCase)

    val sd = new StorageDescriptor()
    columnFieldSchemas.foreach(f => sd.addToCols(f))

    location.fold(table.setTableType(TableType.MANAGED_TABLE.toString()))(p => {
      table.setTableType(TableType.EXTERNAL_TABLE.toString())
      //Copied from cascading-hive - Need to set this as well since setting the table type would be too obvious
      table.putToParameters("EXTERNAL", "TRUE")
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

  def createTable[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
      database: String,
      table: String,
      location: Option[Path] = None): Hive[Boolean] = {
    Hive
      .createDatabase(database)
      .flatMap(_ => Hive.existsTable(database, table))
      .flatMap(exists => {
        if (exists)
          Hive
            .mandatory(
              existsTableStrict[T, P](database, table, location),
              s"$database.$table already exists but has different schema."
            )
            .map(_ => false)
        else {
          Hive.getConfClient.flatMap({
            case (conf, client) => {
              val fqLocation =
                location.map(FileSystem.get(conf).makeQualified(_))
              val metadataTable =
                HiveMetadataTable[T, P](database, table, fqLocation)

              try {
                client.createTable(metadataTable)
                Hive.value(true)
              } catch {
                case _: AlreadyExistsException =>
                  Hive
                    .mandatory(
                      existsTableStrict[T, P](database, table, location),
                      s"$database.$table already exists but has different schema."
                    )
                    .map(_ => false)
                case NonFatal(t) =>
                  Hive.error(s"Failed to create table $database.$table", t)
              }
            }
          })
        }
      })
  }

  def existsTableStrict[T <: ThriftStruct: Manifest: HasStructType, P: Partitions](
      database: String,
      table: String,
      location: Option[Path]): Hive[Boolean] =
    Hive((conf, client) =>
      try {
        val fs          = FileSystem.get(conf)
        val actualTable = client.getTable(database, table)
        val expectedTable =
          HiveMetadataTable[T, P](database, table, location.map(fs.makeQualified(_)))
        val actualCols = actualTable.getSd.getCols.asScala
          .map(c => (c.getName.toLowerCase, c.getType.toLowerCase))
          .toList
        val expectedCols = expectedTable.getSd.getCols.asScala
          .map(c => (c.getName.toLowerCase, c.getType.toLowerCase))
          .toList

        //partition keys of unpartitioned table comes back from the metastore as an empty list
        val actualPartCols = actualTable.getPartitionKeys.asScala
          .map(pc => (pc.getName.toLowerCase, pc.getType.toLowerCase))
          .toList

        //partition keys of unpartitioned table not submitted to the metastore will be null
        val expectedPartCols = Option(expectedTable.getPartitionKeys)
          .map(_.asScala.map(pc => (pc.getName.toLowerCase, pc.getType.toLowerCase)).toList)
          .getOrElse(List.empty)
        val warehouse = conf.getVar(ConfVars.METASTOREWAREHOUSE)
        val actualPath =
          fs.makeQualified(new Path(actualTable.getSd.getLocation))

        //Do we want to handle `default` database separately
        val expectedLocation =
          Option(expectedTable.getSd.getLocation).getOrElse(
            s"$warehouse/${expectedTable.getDbName}.db/${expectedTable.getTableName}"
          )
        val expectedPath = fs.makeQualified(new Path(expectedLocation))

        val delimiterComparison = true // because of the Parquet format.

        Result.ok(
          actualTable.getTableType == expectedTable.getTableType &&
            actualPath == expectedPath &&
            actualCols == expectedCols &&
            actualPartCols == expectedPartCols &&
            actualTable.getSd.getInputFormat == expectedTable.getSd.getInputFormat &&
            actualTable.getSd.getOutputFormat == expectedTable.getSd.getOutputFormat &&
            delimiterComparison
        )
      } catch {
        case _: NoSuchObjectException => Result.ok(false)
        case NonFatal(t) =>
          Result.error(s"Failed to check strict existence of $database.$table", t)
    })
}
