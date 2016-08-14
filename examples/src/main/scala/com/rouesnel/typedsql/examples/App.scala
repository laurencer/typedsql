package com.rouesnel.typedsql.examples

import org.apache.commons.configuration.Configuration
import org.apache.hadoop.hive.conf.HiveConf

import com.twitter.scalding._

import com.rouesnel.typedsql.{DataSource, SqlQuery, TypedPipeDataSource, UDF}
import DataSource.Strategy._

import com.rouesnel.typedsql.examples.coppersmith._

@SqlQuery object Step1 {
  def query =
    """
      SELECT "Bob" as firstname,
             "Brown" as lastname,
              28 as age
    """
}

@SqlQuery object Step2 {

  @UDF def joinNames(first: String, last: String): String = s"${last}, ${first}"

  @UDF def birthyear(age: Int): Int = 1982

  def query(step1: DataSource[Step1.Row]) =
    """
      SELECT *,
             joinNames(firstname, lastname) as full_name,
             birthyear(age) as born
      FROM ${step1}
    """
}

object App extends ExecutionApp {

  def job = Execution.getConfigMode.flatMap({ case (appConfig, Hdfs(_, conf)) => {

    val customers: DataSource[Customer] = TypedPipeDataSource(
      TypedPipe.from(IterableSource(
        Customer(123, "Bob",   "Brown", date, "bb@email.com", Some("MALE"), 32)
      , Customer(124, "Diana", "Brown", date, "db@email.com", Some("FEMALE"), 31)
      , Customer(125, "Aaron", "Brown", date, "ab@email.com", Some("FEMALE"), 14)
      ))
    )

    val orders: DataSource[Order] = TypedPipeDataSource(
      TypedPipe.from(IterableSource(
        Order("O_000098", 123, date, None, None, None)
      ))
    )

    val step1: DataSource[Step1.Row] =
      Step1.query.persist(reuseExisting("step1", "example.step1", "examples/step1"))

    val step2: DataSource[Step2.Row] =
      Step2.query(step1)
        .persist(flaggedReuse("step2", "example.step2", "examples/step2"))

    def config = DataSource.defaultConfig(
      conf = new HiveConf(conf, classOf[Configuration]),
      args = appConfig.getArgs.m
    )

    step2.toHiveTable(config).onComplete(tried => {
      println(tried)
    }).map(_ => ())
  }})
}

object FakeData {
  import scala.util.Random

  def fakeFirstName(r: Random) = ???
  def fakeLastName(r: Random) = ???
  def fakeEmailAddress(r: Random) = ???
  def fakeGender(r: Random): Option[String] = r.shuffle(
    None,
    Some(""),
    Some("m"),
    Some("M"),
    Some("male"),
    Some("Male"),
    Some("MALE"),
    Some("f"),
    Some("F"),
    Some("female"),
    Some("Female"),
    Some("FEMALE")
  ).head

  def fakeAge(r: Random) = r.nextInt(60) + 15

  def customer(r: Random) = Customer(
    math.abs(r.nextInt()),
    fakeFirstName(r),
    fakeLastName(r),
    fakeEmailAddress(r),
    fakeGender(r),
    fakeAge(r)
  )
}
