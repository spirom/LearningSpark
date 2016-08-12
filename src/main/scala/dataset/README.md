# _Dataset_ Examples

The Dataset was introduced in Spark 1.6.0 but has really come to fruition in Spark 2.0.0. It
provides a strongly typed, and largely statically typed query/transformation interface.

Note that interoperability with DataFrame is very important -- see
[dataframe/DatasetConversion.scala](../dataframe/DatasetConversion.scala) for
details.


## Getting started

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| Basic.scala           | How to create a `DataSet`, examine it and perform basic operations. **Start here.** |
| CaseClass.scala       | A `DataSet` is more convenient to use if you define a case class for the element type. |
