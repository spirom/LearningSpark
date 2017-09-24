# _Dataset_ Examples

The Dataset was introduced in Spark 1.6.0 but has really come to fruition in Spark 2.0.0. It
provides a strongly typed, and largely statically typed query/transformation interface.

The key to understanding the Dataset concept is to see how naturally
Scala case classes can be used to define simple or complex that can then be queried
using either the Dataset/DataFrame query DSL or SQL or both.

Note that interoperability with DataFrame is very important -- see
[dataframe/DatasetConversion.scala](../dataframe/DatasetConversion.scala) for
details.


## Getting started

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| Basic.scala           | How to create a `DataSet`, examine it and perform basic operations. **Start here.** |
| CaseClass.scala       | A `DataSet` is more convenient to use if you define a case class for the element type. |

## Advanced

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| ComplexType.scala   | Creating a Dataset with various forms of complex schema (nesting, arrays, maps, nullability), based on case classes |
| PartitionBy.scala | Using partitionBy() when writing a DataSet to organize the output into a directory hierarchy. |