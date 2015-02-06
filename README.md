# The _LearningSpark_ Project

This project contains snippets of Scala code for illustrating various
Apache Spark concepts. It is
intended to help you _get started_ with learning Spark by providing a super easy on-ramp that _doesn't_ involve Unix, cluster configuration, building from sources or
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

<table>
<tr><th>Package</th><th>File</th><th>What's Illustrated</th></tr>
<tr><td></td><td>Ex1_SimpleRDD</td><td></td></tr>
<tr><td></td><td>Ex2_Computations</td><td></td></tr>
<tr><td></td><td>Ex3_CombinignRDDs</td><td></td></tr>
<tr><td></td><td>Ex4_MoreOperationsOnRDDs</td><td></td></tr>
<tr><td></td><td>Ex5_Partitions</td><td></td></tr>
<tr><td></td><td>Ex6_Accumulators</td><td></td></tr>
<tr><td>special</td><td>CustomPartitioner</td><td></td></tr>
<tr><td>special</td><td>HashJoin</td><td></td></tr>
<tr><td>special</td><td>PairRDD</td><td></td></tr>
<tr><td>sql</td><td>JSON</td><td></td></tr>
<tr><td>sql</td><td>OutputJSON</td><td></td></tr>
<tr><td>sql</td><td>UDF</td><td></td></tr>
<tr><td>sql</td><td>CustomRelationProvider</td><td></td></tr>
<tr><td>sql</td><td>RelationProviderFilterPushdown</td><td></td></tr>
<tr><td>sql</td><td>ExternalNonRectangular</td><td></td></tr>
<tr><td>streaming</td><td>FileBased</td><td></td></tr>
<tr><td>streaming</td><td>QueueBased</td><td></td></tr>
<tr><td>streaming</td><td>Accumulation</td><td></td></tr>
<tr><td>streaming</td><td>Windowing</td><td></td></tr>
</table>

Additional Scala code is "work in progress". 
