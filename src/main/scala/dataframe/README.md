# _DataFrame_ Examples

`DataFrame` was introduced in Spark 1.3.0 to replace and generalize `SchemaRDD`. 
There is one significant difference between the two classes: while `SchemaRDD` extended `RDD[Row]`, `DataFrame` contains one. 
In most cases, where your code used a `SchemaRDD` in the past, it can now use a `DataFrame` without additional changes. 
However, the domain-specific language (DSL) for querying through a `SchemaRDD` without writing SQL has been strengthened considerably in the `DataFrame` API. 

Two important additional classes to understand are `Row` and `Column`.

This directory contains examples of using `DataFrame`, focusing on the aspects that are not strictly related to Spark SQL queries --
the specifically SQL oriented aspects are covered in the **sql** directory, which is a peer of this one. 

## Getting started

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| Basic.scala           | How to create a `DataFrame` from case classes, examine it and perform basic operations. **Start here.** |
| SimpleCreation.scala | Create essentially the same `DataFrame` as in Basic.scala from an `RDD` of tuples, and explicit column names. |
| FromRowsAndSchema.scala | Create essentially the same `DataFrame` as in Basic.scala from an `RDD[Row]` and a schema. |

## Querying

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| GroupingAndAggrgegation.scala | Several different ways to specify aggregation. |
| Select.scala          | How to extract data from a `DataFrame`. This is a good place to see how convenient the API can be. |
| DateTime.scala        | Functions for querying against DateType and DatetimeType. |

## Utilities

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| DropColumns.scala     | Creating a new DataFrame that omits some of the original DataFrame's columns. |
| DropDuplicates        | Crate a DataFrame that removes duplicate rows, or optionally removes rows that are only identical on certain specified columns. |
| Range.scala           | Using range() methods on SQLContext to create simple DataFrames with values from that range. |

## Advanced

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| ComplexSchema.scala   | Creating a DataFrame with various forms of complex schema -- start with FromRowsAndSchema.scala for a simpler example |
| Transform.scala       | How to transform one `DataFrame` to another: written in response to a [question on StackOverflow](http://stackoverflow.com/questions/29151348/operation-on-data-frame/29159604). |
| UDF.scala             | How to use user-defined functions (UDFs) in queries. Note that the use of UDFs in SQL queries is covered seperately in the **sql** directory. |
| UDT.scala             | User defined types from a DataFrame perspective -- depends on understanding UDF.scala |
| UDAF.scala            | A simple User Defined Aggregation Function as introduced in Spark 1.5.0 |
| DatasetConversion.scala | Explore interoperability between DataFrame and Dataset  -- note that Dataset is convered more detail in [dataset](../dataset/README.md) |