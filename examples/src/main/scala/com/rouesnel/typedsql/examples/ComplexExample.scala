package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql.DataSource.Strategy._
import com.rouesnel.typedsql._, core._
import com.twitter.scalding._
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.slf4j.LoggerFactory


@SqlQuery object ComplexExample {

  def query(upstream: DataSource[ComplexValue]) =
    """
      SELECT data_id, value
      FROM ${upstream}
      LATERAL VIEW explode(data) e
    """
}

object ComplexExampleApp extends ExecutionApp {
  val log = LoggerFactory.getLogger(getClass)

  def job = Execution.getConfigMode.flatMap({ case (appConfig, Hdfs(_, conf)) => {
    val source: DataSource[ComplexValue] =
      TypedPipeDataSource(
        TypedPipe.from(
          IterableSource(List(
            ComplexValue(Map(
              "a" -> ValueType(Some("str"), None, None, None, None, 1),
              "b" -> ValueType(None, None, Some(1), None, None, 1)
            ), "100"),
            ComplexValue(Map(
              "a" -> ValueType(Some("str"), None, None, None, None, 1),
              "b" -> ValueType(None, None, Some(1), None, None, 1)
            ), "101"),
            ComplexValue(Map(
              "a" -> ValueType(Some("str"), None, None, None, None, 1),
              "b" -> ValueType(None, None, Some(1), None, None, 1)
            ), "102"),
            ComplexValue(Map(
              "a" -> ValueType(Some("str"), None, None, None, None, 1),
              "b" -> ValueType(None, None, Some(1), None, None, 1)
            ), "103"),
            ComplexValue(Map(
              "a" -> ValueType(Some("str"), None, None, None, None, 1),
              "b" -> ValueType(None, None, Some(1), None, None, 1)
            ), "104")
          )
      ))).persist(alwaysRefresh("source", "examples.source", "complex_source"))

    val complexStep =
      ComplexExample
        .query(source)
        .persist(alwaysRefresh("complex", "examples.complex1", "complex1"))

    def config = DataSource.defaultConfig(
      conf = new HiveConf(conf, classOf[Configuration]),
      args = appConfig.getArgs.m
    )

    complexStep.toHiveTable(config).onComplete(tried => {
      println(tried.get)
    }).map(_ => ())
  }})
}
