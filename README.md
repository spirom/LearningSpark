# The _LearningSpark_ Project

This project contains snippets of Scala code for illustrating various
Apache Spark concepts. It is
intended to help you _get started_ with learning Apache Spark (as a _Scala_ programmer) by providing a super easy on-ramp that _doesn't_ involve Unix, cluster configuration, building from sources or
installing Hadoop. Many of these activities will be necessary later in your
learning experience, after you've used these examples to achieve basic familiarity.

It is intended to accompany a number of posts on the blog
[A River of Bytes](http://www.river-of-bytes.com).

## Dependencies

The project was created with IntelliJ Idea 14 Community Edition,
JDK 1.7, Scala 2.11.2 and Spark 1.2.0 on Windows 8. 

Versions of these examples for other configurations (older versions of Scala and Spark) can be found in various branches.

## Examples

The examples can be found under src/main/scala. The best way to use them is to start by reading the code and its comments. Then, since each file contains an object definition with a main method, run it and consider the output. Relevant blog posts and StackOverflow answers are listed below. 

| Package | File                  | What's Illustrated    |
|---------|-----------------------|-----------------------|
|         | Ex1_SimpleRDD         | How to execute your first, very simple, Spark Job. See also [An easy way to start learning Spark](http://www.river-of-bytes.com/2014/11/an-easy-way-to-start-learning-spark.html).
|         | Ex2_Computations      | How RDDs work in more complex computations. See also [Spark computations](http://www.river-of-bytes.com/2014/11/spark-computations.html). |
|         | Ex3_CombiningRDDs     | Operations on multiple RDDs |
|         | Ex4_MoreOperationsOnRDDs | More complex operations on individual RDDs |
|         | Ex5_Partitions        | Explicit control of partitioning for performance and scalability. |
|         | Ex6_Accumulators | How to use Spark accumulators to efficiently gather the results of distributed computations. |
| special | CustomPartitioner | How to take control of the partitioning of an RDD. |
| special | HashJoin         | How to use the well known Hash Join algorithm to join two RDDs where one is small enough to entirely fit in the memory of each partition. See also [this question on StackOverflow](http://stackoverflow.com/questions/26238794/spark-join-exponentially-slow) |
| special | PairRDD | How to operate on RDDs in which the underlying elements are pairs. |
| sql | JSON | Use Spark SQL to query JSON text |
| sql | OutputJSON | Write the result of a query against JSON back out as JSON text. This functionality is built into Spark 1.2.0, but the example was wrtitten to answer [this question on StackOverflow](http://stackoverflow.com/questions/26737251/pyspark-save-schemardd-as-json-file) in the days of Spark 1.1.0. |
| sql | UDF | How to use Scala "user defined functions" (UDFs) in Spark SQL. See [this question on StackOverflow](http://stackoverflow.com/questions/25031129/creating-user-defined-function-in-spark-sql). |
| sql | CustomRelationProvider | How to use the external data source provider for simple integration with an external database engine. See the
blog post [External Data Sources in Spark 1.2.0](http://www.river-of-bytes.com/2014/12/external-data-sources-in-spark-120.html).| 
| sql | RelationProviderFilterPushdown | More advanced integration using the external data source API, enabling filter and projection pushdown. See the blog post [Filtering and Projection in Spark SQL External Data Sources](http://www.river-of-bytes.com/2014/12/filtering-and-projection-in-spark-sql.html).| 
| sql | ExternalNonRectangular | An illustration that the Spark SQL query compiler doesn't make much use of the above pushdown possibilities in the
presence of a non-rectangular Schema, like that inferred from JSON data.|
<tr><td>streaming</td><td>FileBased</td><td></td></tr>
<tr><td>streaming</td><td>QueueBased</td><td></td></tr>
<tr><td>streaming</td><td>Accumulation</td><td></td></tr>
<tr><td>streaming</td><td>Windowing</td><td></td></tr>


Additional Scala code is "work in progress". 
