package com.rouesnel.typedsql.examples

import java.util.Date

import com.twitter.scalding._
import au.com.cba.omnia.beeswax.Hive
import com.rouesnel.typedsql.{DataSource, SqlQuery, UDF}
import DataSource.Strategy._
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.hive.conf.HiveConf

import scala.util.Random

@SqlQuery object Step1 {

  case class Parameters()
  case class Sources()
  def query =
    """
      SELECT "Bob" as firstname,
             "Brown" as lastname,
              28 as age
    """
}

@SqlQuery object Step2 {

  case class Parameters()
  case class Sources(step1: DataSource[Step1.Row])

  @UDF def joinNames(first: String, last: String): String = s"${last}, ${first}"

  @UDF def birthyear(age: Int): Int = 1982

  def query =
    """
      SELECT *,
             joinNames(firstname, lastname) as full_name,
             birthyear(age) as born
      FROM ${step1}
    """
}

object App extends ExecutionApp {


  def job = Execution.getConfigMode.flatMap({ case (appConfig, Hdfs(_, conf)) => {
    val step1: DataSource[Step1.Row] = Step1(
      Step1.Sources(),
      Step1.Parameters()
    ).persist(reuseExisting("step1", "example.step1", "examples/step1"))

    val step2: DataSource[Step2.Row] = Step2(
      Step2.Sources(step1),
      Step2.Parameters()
    ).persist(flaggedReuse("step2", "example.step2", "examples/step2"))

    def config = DataSource.defaultConfig(
      conf = new HiveConf(conf, classOf[Configuration]),
      args = appConfig.getArgs.m
    )

    step2.toHiveTable(config).onComplete(tried => {
      println(tried)
    }).map(_ => ())
  }})
}
