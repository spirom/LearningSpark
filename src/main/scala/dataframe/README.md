# _DataFrame_ Examples

`DataFrame` was introduced in Spark 1.3.0 to replace and generalize `SchemaRDD`. 
There is one significant difference between the two classes: while `SchemaRDD` extended `RDD[Row]`, `DataFrame` contains one. 
In most cases, where your code used a `SchemaRDD` in the past, it can now use a `DataFrame` without additional changes. 
However, the domain-specific language (DSL) for querying through a `SchemaRDD` without writing SQL has been strengthened considerably in the `DataFrame` API. 

Two important additional classes to understand are `Row` and `Column`.

This directory contains examples of using `DataFrame`, focusing on the aspects that are not stricly related to Spark SQL queries -- 
the specifically SQL oriented aspects are covered in the **sql** directory, which is a peer of this one. 



| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| Basic.scala           | How to create a `DataFrame` from case classes, examine it and perform basic operations. **Start here.** |
| Select.scala          | How to extract data from a `DataFrame`. This is a good place to see how convenient the API can be. |
| Transform.scala       | How to transform one `DataFrame` to another: written in response to a [question on StackOverflow](http://stackoverflow.com/questions/29151348/operation-on-data-frame/29159604). |
| UDF.scala             | How to use user-defined functions (UDFs) in queries. Note that the use of UDFs in SQL queries is covered seperately in the **sql** directory. |