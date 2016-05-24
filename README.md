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
JDK 1.7, Scala 2.11.2 and Spark 1.3.0 on Windows 8.

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
| [hiveql](src/main/scala/hiveql)  |  | Using HiveQL features in a HiveContext. See the local README.md in that directory for details. |
| special | CustomPartitioner | How to take control of the partitioning of an RDD. |
| special | HashJoin         | How to use the well known Hash Join algorithm to join two RDDs where one is small enough to entirely fit in the memory of each partition. See also [this question on StackOverflow](http://stackoverflow.com/questions/26238794/spark-join-exponentially-slow) |
| special | PairRDD | How to operate on RDDs in which the underlying elements are pairs. |
| [dataframe](src/main/scala/dataframe) |  | A range of DataFrame examples -- see the local README.md in that directory for details. |
| experiments | | Experimental examples that may or may not lead to anything interesting. |
| [sql](src/main/scala/sql) | | A range of SQL examples -- see the local README.md in that directory for details.  |
| [streaming](src/main/scala/streaming) | | Streaming examples -- see the local README.md in that directory for details.  |
| [graphx](src/main/scala/graphx) | | A range of GraphX examples -- see the local README.md in that directory for details. |


Additional Scala code is "work in progress". 
