package com.rouesnel.typedsql

import java.io.File

import scala.concurrent._
import scala.language.experimental.macros

import org.apache.hadoop.hive.conf.HiveConf

import scalaz._, Scalaz._

/** Provides functions to support a fake Hive environment */
object HiveSupport {
  import scala.concurrent.ExecutionContext.Implicits.global

  def useHiveClassloader[T](f: => T): T = {
    // Replace the classloader with the correct path.
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(classOf[HiveConf].getClassLoader)
    // Run the computation
    val result = \/.fromTryCatchNonFatal(f)
    // Reset the contextClassLoader.
    Thread.currentThread().setContextClassLoader(contextClassLoader)
    // Return the result
    result match {
      case \/-(value) => value
      case -\/(error) => throw error
    }
  }

  val noopLogger: String => Unit = _ => ()

  def initialize(changeClassloader: Boolean = true, log: String => Unit = noopLogger): Future[HiveConf] = Future {
    log("Starting to initialize Hive.")
    def _initialize = {
      import org.apache.hadoop.hive.conf.HiveConf, HiveConf.ConfVars, ConfVars._
      import org.apache.hadoop.hive.metastore._, api._

      log("Creating temporary directory for Hive.")
      val tempDir = File.createTempFile("hive", "compile")
      tempDir.delete()
      tempDir.mkdir()
      log(s"Temporary directory: ${tempDir}")

      /* Creates a fake local instance for Hive - stolen from Thermometer Hive */
      lazy val hiveDir: String       = tempDir.toString
      lazy val hiveDb: String        = s"$hiveDir/hive_db"
      lazy val hiveWarehouse: String = s"$hiveDir/warehouse"
      lazy val derbyHome: String     = s"$hiveDir/derby"
      lazy val hiveConf: HiveConf    = new HiveConf <| (conf => {
        conf.setVar(METASTOREWAREHOUSE, hiveWarehouse)
      })

      // Export the warehouse path so it gets picked up when a new hive conf is instantiated somehwere else.
      System.setProperty(METASTOREWAREHOUSE.varname, hiveWarehouse)
      System.setProperty("derby.system.home", derbyHome)
      // Export the derby db file location so it is different for each test.
      System.setProperty("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$hiveDb;create=true")
      System.setProperty("hive.metastore.ds.retry.attempts", "0")

      // Wait for the Hive client to initialise
      log(s"Creating Hive Client.")
      val client = RetryingMetaStoreClient.getProxy(
        hiveConf,
        new HiveMetaHookLoader() {
          override def getHook(tbl: Table) = null
        },
        classOf[HiveMetaStoreClient].getName()
      )
      try {
        log("Checking if Hive is initialised (by getting all databases)")
        client.getAllDatabases()
        log("Hive is initialised properly.")
      } finally {
        log("Closing Hive Client.")
        client.close
        log("Hive client closed.")
      }

      log("Creating placeholder tables.")
      import au.com.cba.omnia.beeswax.Hive
      val successfullyCreated = Hive.createParquetTable[Person]("test_db", "test", Nil)
        .run(hiveConf)
      log("Placeholder tables created successfully.")
      log(s"Created test table: $successfullyCreated")

      hiveConf
    }
    if (changeClassloader) {
      useHiveClassloader(_initialize)
    } else {
      _initialize
    }
  }
}
