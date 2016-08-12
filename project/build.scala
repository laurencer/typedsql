import sbt._, Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._, depend.versions
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

import sbtassembly.AssemblyPlugin.autoImport._

object build extends Build {

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformPublicDependencySettings ++
    Seq(
      concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
      dependencyOverrides += "com.chuusai" %% "shapeless" % "2.2.5" //until maestro is updated
    )

  lazy val publishSettings =
    Seq(
      organization := "com.rouesnel"
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
        ++ publishSettings
        ++ Seq(
        publishArtifact := false
      )
    , aggregate = Seq(core, macros, test, examples)
  )

  lazy val core: Project = Project(
    id = "core"
    , base = file("core")
    , settings =
      standardSettings
        ++ uniform.project("typedsql-core", "com.rouesnel.typedsql")
        ++ publishSettings
        ++ Seq(
        libraryDependencies ++= depend.scalaz() ++ Seq(
          "com.lihaoyi" %% "fastparse" % "0.3.7"
        )
      )
  )

  lazy val macros: Project = Project(
    id = "macro"
    , base = file("macro")
    , settings =
      standardSettings
        ++ uniform.project("typedsql-macro", "com.rouesnel.typedsql")
        ++ uniformThriftSettings
        ++ macroBuildSettings
        ++ publishSettings
        ++ Seq(
        libraryDependencies ++=
          depend.hadoopClasspath ++
          depend.omnia("ebenezer", "0.22.2-20160619063420-4eb964f") ++
          depend.parquet() ++
          depend.testing() ++
          depend.logging() ++
          depend.hadoop() ++
          depend.hive() ++
          Seq(
            "au.com.cba.omnia"        %% "thermometer-hive" % "1.4.2-20160414053315-99c196d",
            "ch.qos.logback"           % "logback-classic"  % "1.0.13",
            "io.argonaut"             %% "argonaut"         % "6.1"
          )
      )
  ) dependsOn(core)

  lazy val coppersmithVersion = "0.21.3-20160724231815-2c523f2"

  lazy val examples = Project(
    id = "examples"
    , base = file("examples")
    , settings =
      standardSettings
        ++ uniform.project("typedsql-examples", "com.rouesnel.typedsql.examples")
        ++ uniformThriftSettings
        ++ macroBuildSettings
        ++ publishSettings
        ++ Seq(
          conflictManager := ConflictManager.default,
          parallelExecution in Test := false,
          // fork in Test := true,
          // fork in Compile := true,
          libraryDependencies ++=
            depend.hadoopClasspath ++
              depend.omnia("ebenezer", "0.22.2-20160619063420-4eb964f") ++
              depend.parquet() ++
              depend.testing() ++
              depend.logging() ++
              depend.hadoop() ++
              depend.hive() ++
              Seq(
                "au.com.cba.omnia" %% "thermometer-hive"     % "1.4.2-20160414053315-99c196d",
                "ch.qos.logback"    % "logback-classic"      % "1.0.13",
                "au.com.cba.omnia" %% "coppersmith-core"     % coppersmithVersion,
                "au.com.cba.omnia" %% "coppersmith-scalding" % coppersmithVersion,
                "au.com.cba.omnia" %% "coppersmith-tools"    % coppersmithVersion
              )
      )
  ) dependsOn(macros, test % "test->compile")

  lazy val test = Project(
    id = "test"
    , base = file("test")
    , settings = standardSettings
      ++ uniform.project("typedsql-test", "com.rouesnel.typedsql.test")
      ++ uniformThriftSettings
      ++ macroBuildSettings
      ++ publishSettings
      ++ Seq(
      libraryDependencies ++=
        depend.hadoopClasspath ++
        depend.omnia("ebenezer", "0.22.2-20160619063420-4eb964f") ++
        depend.parquet() ++
        depend.testing() ++
        depend.logging() ++
        depend.hadoop() ++
        depend.hive() ++
        depend.testing(configuration = "compile") ++
        Seq(
        "au.com.cba.omnia" %% "thermometer-hive" %  "1.4.2-20160414053315-99c196d"
      )
    )
  ) dependsOn(macros)
}
