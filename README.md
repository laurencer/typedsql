# TypedSQL

> Making HiveQL testable, type-safe and composable by embedding/lifting it in to Scala.

**This is a hacky work-in-progress. It is not well-structured, documented or usable and represents an exploration into various Scala and Hive APIs.**

By adding the `@SqlQuery` annotation to an object, TypedSQL will generate a [Parquet](https://github.com/Parquet/parquet-mr)/[Scrooge](https://github.com/twitter/scrooge) schema for the contained `query`.

```scala

@SqlQuery object TransactionSummary {
  case class Sources(
    transactions: DataSource[Transaction],
    accounts:     DataSource[ProductAccount],
    customers:    DataSource[Customer]
  )

  case class Parameters(
    transactionType: String,
    year:            Int
  )

  def query =
    """
      SELECT c.id           as customer_id,
             MONTH(t.date)  as month,
             SUM(t.value)   as amount_spent,
             COUNT(t.id)    as transaction_count
      FROM ${transactions} t
        INNER JOIN ${accounts} a ON t.account_id = a.id
        INNER JOIN ${customers} c ON a.primary_customer_id = c.id
      WHERE t.type = ${transactionType} AND t.year = ${year}
      GROUP BY c.id, MONTH(t.date)
    """
}

```

The `query` is checked for validity at compile time (e.g. if you have a syntax error or reference an invalid source/parameter then it will be caught by the Scala compiler).

The macro also adds a new case class to `TransactionSummary` called `Row` which represents the output schema of the query and looks something like (nb. field names are converted to *Camel Case* for ease of use):

```scala
case class Row(
  customerId:       Int,
  month:            Int,
  amountSpent:      Long,
  transactionCount: Long
)
```

The `Row` case class also has a companion object extending `ThriftStructCodec3[Row]` which allows it to be used like any other Scrooge generated class. The outputs of the query can be read from Parquet output using [Scalding](https://github.com/twitter/scalding) with Parquet support provided by [Ebenezer](https://github.com/CommBank/ebenezer/).

```scala

val transactionSummaries: TypedPipe[TransactionSummary.Row] = ParquetScroogeSource[TransactionSummary.Row]("/my/table/location").map(row => {
  val customerId: Int = row.customerId
  val month: Int = row.month
  val amountSpent: Long = row.amountSpent
  val transactionCount: Long = row.transactionCount
  row
})

```

If the query is changed and a field is removed or its type is changed then it will be caught by the Scala compiler if its referenced downstream directly. This also works across multiple queries (e.g. if the `customerId` field is changed to a `String` then this will result in a compile-time error).

## Reusing Queries

The `Row` class generated on the object is usable directly in Scalding flows or by other TypedSQL queries.

```scala

@SqlQuery object WebClickTransactionAnalysis {
  case class Sources(
    webClicks:            DataSource[WebClick],
    transactionSummaries: DataSource[TransactionSummary.Row]
  )

  case class Parameters()

  def query =
    """
      SELECT ts.id    as customer_id,
             ts.month as month,
             ts.*,
             wcs.*
      FROM ${transactionSummaries} ts
        OUTER JOIN ${webClicks} wcs ON ts.customer_id = wcs.customer_id AND ts.month = wcs.month
    """
}
```

## Debugging

Setting the `PRINT_TYPEDSQL_CLASSES` environment variable for your `sbt` session will cause
TypedSQL to print all generated/modified objects out during compilation.
