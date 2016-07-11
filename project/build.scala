import sbt._, Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._, depend.versions
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

object build extends Build {
  val maestroVersion = "2.20.0-20160520031836-e06bc75"

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformPublicDependencySettings ++
    strictDependencySettings ++
    Seq(
      concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
      dependencyOverrides += "com.chuusai" %% "shapeless" % "2.2.5" //until maestro is updated
    )

  lazy val macroBuildSettings = Seq(
    libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
    libraryDependencies ++= {
      if (scalaBinaryVersion.value == "2.10") Seq(
        compilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full),
        "org.scalamacros" %% "quasiquotes" % "2.0.0" cross CrossVersion.binary
      ) else Nil
    },
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
  )

  lazy val all = Project(
    id = "all"
    , base = file(".")
    , settings =
      standardSettings
        ++ uniform.project("typedsql-all", "com.rouesnel.typedsql.all")
        ++ Seq(
        publishArtifact := false
      )
    , aggregate = Seq(core)
  )

  lazy val core = Project(
    id = "core"
    , base = file("core")
    , settings =
      standardSettings
        ++ uniform.project("typedsql-core", "com.rouesnel.typedsql")
        ++ uniformThriftSettings
        ++ macroBuildSettings
        ++ Seq(
        conflictManager := ConflictManager.default,
        libraryDependencies ++=
          depend.hadoopClasspath ++
          depend.omnia("maestro-test", maestroVersion, "test") ++
          depend.omnia("beeswax", "0.1.1-20160120052815-6bf77d2") ++
          depend.parquet() ++
          depend.testing() ++
          depend.hadoop() ++
          depend.hive() ++
          depend.omnia("maestro-test", maestroVersion, "test") ++
          Seq(
            "au.com.cba.omnia" %% "thermometer-hive" %  "1.4.2-20160414053315-99c196d"
          )
      )
  )
}
