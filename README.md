# TypedSQL

> Making HiveQL testable, type-safe and composable by embedding/lifting it in to Scala.

**This is a hacky work-in-progress. It is not well-structured, documented or usable and represents an exploration into various Scala and Hive APIs.**

By adding the `@SqlQuery` annotation to an object, TypedSQL will generate a [Parquet](https://github.com/Parquet/parquet-mr)/[Scrooge](https://github.com/twitter/scrooge) schema for the contained `query`.

```scala

@SqlQuery object TransactionSummary {
  def query(transactionType: String, year: Int)
           (transactions: DataSource[Transaction],
            accounts:     DataSource[ProductAccount],
            customers:    DataSource[Customer]) =
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

The `query` is checked for validity at compile time (e.g. if you have a syntax error or reference an invalid source/parameter then it will be caught by the Scala compiler) and replaced with a function that returns a usable `DataSource[Row]`.

The parameters to the `query` method are either (*note - multiple parameter groups are supported and they are treated equally*):

- Data Sources (any parameter of the type `DataSource[_]`): these represent the upstream tables used by the query.
- Variables (all other parameters): these are variables that available in the body of the query.

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

## Using DataSources

`DataSource[T]`s represent a query that can produce a dataset on HDFS (similar to Scalding's `TypedPipe[T]`s). There are two main ways to use a `DataSource[T]`:

- `DataSource#toTypedPipe(config): Execution[TypedPipe[T]]`
- `DataSource#toHiveTable(config): Execution[MaterialisedHiveTable[T]]`

The `MaterialisedHiveTable` provides access to the name of the output Hive table (`hiveTable`) which can then be accessed through normal means.

`DataSource[T]`s also have a few neat tricks:

- They can automatically 


## Reusing Queries

The `Row` class generated on the object is usable directly in Scalding flows or by other TypedSQL queries.

```scala

@SqlQuery object WebClickTransactionAnalysis {
  def query(webClicks: DataSource[WebClick])
           (transactionSummaries: DataSource[TransactionSummary.Row]) =
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

Since the `query` method is a function it can be composed by regular means.

```

(TransactionSummary.query _).tupled andThen (WebClickTransactionAnalysis(wc) _)

```

## UDFs (experimental)

User-defined functions in Hive allow a query to execute a Java function as if it were built-in to
SQL/Hive. TypedSQL provides an easy way to create and use UDFs in your HiveQL by annotating regular
Scala functions inside the `@SqlQuery` object with the `@UDF` annotation.

```scala

@SqlQuery object TransactionAnalysis {
  @UDF convertTransactionId(id: String): String = 
    "TRANS_" + id.split("\\.").last

  def query(transactions: DataSource[Transaction],
            accounts:     DataSource[ProductAccount]) =
    """
      SELECT c.id                       as customer_id,
             t.*,
             convertTransactionId(t.id) as normalized_id
      FROM ${transactions} t
        INNER JOIN ${accounts} a ON t.account_id = a.id
    """
}

```

The `@UDF` macro will generate a `GenericUDF` implementation for the Scala function behind the 
scenes and automatically register the function when executing the queries.

## Examples

A number of examples are included in the `examples` sub-project (these are also used as test cases). 

Some examples can be run on a real Hadoop cluster by executing `./sbt examples/assembly` and then
copying the jars in the `examples/target/scala-2.11/*.jar` to your cluster.

These can then be run using:

```
RUN_DIRECTORY=`pwd`
HADOOP_CLASSPATH=${HIVE_HOME}/conf:${HADOOP_CLASSPATH} hadoop jar typedsql-examples-assembly-0.1.0-SNAPSHOT.jar com.rouesnel.typedsql.examples.App --hdfs -libjars ${RUN_DIRECTORY}/datanucleus-api-jdo-3.2.6.jar,${RUN_DIRECTORY}/datanucleus-core-3.2.10.jar,${RUN_DIRECTORY}/datanucleus-rdbms-3.2.9.jar

```

## Release Guide

Releases are automatically handled by the TravisCI build using an approach
similar to [what the Guardian use](https://www.theguardian.com/info/developer-blog/2014/sep/16/shipping-from-github-to-maven-central-and-s3-using-travis-ci).

On a successful build on master, [sbt-sonatype](https://github.com/xerial/sbt-sonatype)
is used to publish a signed build to [Sonatype OSS](https://oss.sonatype.org/),
which is then released (using the same plugin). The releases are signed with a PGP key (included in the repository in an
encrypted form).

### Credentials

Credentials are included in the repository in the following encrypted files:

- `credentials.sbt.enc`: an encrypted `sbt` file that contains credentials for
  uploading to Sonatype OSS and the passphrase for the PGP keyring.
- `pubring.gpg.enc`: an encrypted gpg public keyring.
- `secring.gpg.enc`: an encrypted gpg private keyring.

During the build these files are decrypted using the `ENCRYPTION_PASSWORD` that
has been encrypted in the `.travis.yml` file.
