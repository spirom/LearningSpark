# _SQL_ Examples

## DataFrame

See the separate [dataframe](../dataframe) for information on the
DataFrame API and especially it's query language.

## Basics

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| Basic | Simple SQL queries. |
| Types | Overview of querying various data types. Note that dates and timestamps are treated separately below. |
| DateTime | Dealing with dates and timestamps. |

## Advanced

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| PartitionedTable      | Creating, updating and querying partitioned tables. |
| ComplexTypes          | Dealing with arrays and nested records in SQL queries |
| UDF                   | How to use Scala "user defined functions" (UDFs) in Spark SQL. See [this question on StackOverflow](http://stackoverflow.com/questions/25031129/creating-user-defined-function-in-spark-sql). |
| UDT.scala             | User defined types from a SQL perspective -- depends on understanding UDF.scala |
| UDAF.scala            | A simple User Defined Aggregation Function as introduced in Spark 1.5.0 |
| UDAF2.scala           | A User Defined Aggregation Function with two parameters |
| UDAF_Multi.scala      | A User Defined Aggregation Function that accumulates and returns multiple values |

## Schema and Rows

## JSON

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| JSON | Basic JSON data. |
| OutputJSON | Write the result of a query against JSON back out as JSON text. This functionality is built into Spark 1.2.0, but the example was written to answer [this question on StackOverflow](http://stackoverflow.com/questions/26737251/pyspark-save-schemardd-as-json-file) in the days of Spark 1.1.0. |
| JSONSchemaInference | Examples of how Spark SQL infers a schema for a file of JSON documents, including multiple cases of schema conflict. |

## External Data Sources

**Note: The following examples use the original external data source API. For the new V2 API introduced in
Spark 2.3.0, see [https://github.com/spirom/spark-data-sources](https://github.com/spirom/spark-data-sources),
which explores the new API in some detail.**

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| CustomRelationProvider | How to use the external data source provider for simple integration with an external database engine. See the blog post [External Data Sources in Spark 1.2.0](http://www.river-of-bytes.com/2014/12/external-data-sources-in-spark-120.html).|
| RelationProviderFilterPushdown | More advanced integration using the external data source API, enabling filter and projection pushdown. See the blog post [Filtering and Projection in Spark SQL External Data Sources](http://www.river-of-bytes.com/2014/12/filtering-and-projection-in-spark-sql.html).|
| ExternalNonRectangular | An illustration that the Spark SQL query compiler doesn't make much use of the above pushdown possibilities in the presence of a non-rectangular Schema, like that inferred from JSON data.|
