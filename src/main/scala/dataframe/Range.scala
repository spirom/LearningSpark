package dataframe

import org.apache.spark.sql.SparkSession

/**
 * Method on SQLContext creating a DataFrame with a single column
 * named "id" of type Long.
  *
  * CAUTION: The overloads of range() have subtly different signatures
  * from the corresponding methods on SparkContext
 */
object Range {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-Range")
        .master("local[4]")
        .getOrCreate()

    println("*** dense range with default partitioning")
    val df1 = spark.range(10, 14)
    df1.show()
    println("# Partitions = " + df1.rdd.partitions.length)

    println("\n*** stepped range")
    val df2 = spark.range(10, 14, 2)
    df2.show()
    println(df2.rdd.partitions.size)

    println("\n*** stepped range with specified partitioning")
    val df3 = spark.range(10, 14, 2, 2)
    df3.show()
    println("# Partitions = " + df3.rdd.partitions.length)

    // Added in Spark 1.4.1. But so far it only seems to have been
    // added to SQLContext and since 2.0.0 on SparkSession -- not available
    // on SparkContext.

    println("\n*** range with just a limit")
    val df4 = spark.range(3)
    df4.show()
    println("# Partitions = " + df4.rdd.partitions.length)
  }
}
