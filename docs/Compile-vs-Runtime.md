# Compile versus Runtime

This page outlines some of the differences between the compile time environment
and the run-time environment.

Take the below query for example:
```sql

SELECT a.*, SUM(t.amount) as total_spend, COUNT(t.id) as transactions_count
FROM ${transactions_table} t
  INNER JOIN ${accounts_table} a ON t.account_id = a.id
WHERE t.date > ${start_date} AND t.date < ${end_date}
GROUP BY a.id

```

- Hive parameters allow the source tables to be dynamically changed (e.g. for different environments such as `dev` and `test`).
- Hive parameters are also used to express filter predicates. In ETL jobs these typically constrict the data processing to a specific batch window.

At compile-time, enough information needs to be known to evaluate the Hive query insofar as necessary to provide its output schema. Notably, the only thing that will effect the output is the schema of the source tables. Somewhat obviously, a specific schema corresponds to what we would think of a as a type in a programming language.

It doesn't matter what values we assign to the filter conditions/parameters at compile-time then (placeholders can simply be used). Similarly, for the source tables themselves, the actual table is irrelevant as long as we have a view or placeholder table that has the same schema as what will run in production.

## Modelling SQL queries as functions

TypedSQL lets us model a HiveQL query as a function that operates on two different kinds of inputs: (1) source/input datasets; and (2) parameters.

```scala

def accountStatistics(start: Date, end: Date)
                     (transaction: List[Transaction], account: List[Account]): List[Query.Row]

```
