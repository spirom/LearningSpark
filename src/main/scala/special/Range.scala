package special

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Method on SparkContext creating an RDD[Long] with a range of values
 * specified by endpoints.
 */
object Range {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-Basic").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //
    // Note: the end value is exclusive
    //
    println("*** dense range with default partitioning")
    val rdd1 = sc.range(10, 14)
    rdd1.collect().foreach(println)
    println("# Partitions = " + rdd1.partitions.length)

    println("*** stepped range with default partitioning")
    val rdd2 = sc.range(10, 14, 2)
    rdd2.collect().foreach(println)
    println("# Partitions = " + rdd2.partitions.length)

    println("*** stepped range with specified partitioning")
    val rdd3 = sc.range(10, 14, 2, 2)
    rdd3.collect().foreach(println)
    println("# Partitions = " + rdd3.partitions.length)

    // Single parameter version available in SQLContext since Spark 1.4.1
    // and on SparkSession since Spark 2.0.0 not available here.
  }
}
