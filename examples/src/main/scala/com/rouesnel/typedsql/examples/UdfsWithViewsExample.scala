package com.rouesnel.typedsql.examples

import com.rouesnel.typedsql._

@SqlQuery
object UdfsWithViewsExampleView {

  @UDF def testConstant(): Boolean            = false
  @UDF def testVariable(name: String): String = s"Happy Birthday ${name}!"

  def query(people: DataSource[Person]) =
    """
      SELECT *, testConstant() as view_constant, testVariable(firstname) as view_variable FROM ${people}
    """
}

@SqlQuery
object UdfsWithViewsExampleTable {

  @UDF def testConstant(): Boolean            = true
  @UDF def testVariable(name: String): String = s"It's not your birthday ${name}!"

  def query(upstream: DataSource[UdfsWithViewsExampleView.Row]) =
    """
      SELECT *, testConstant() as table_constant, testVariable(firstname) as table_variable FROM ${upstream}
    """
}
