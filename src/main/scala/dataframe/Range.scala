package dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Method on SQLContext creating a DataFrame with a single column
 * named "id" of type Long.
 *
 * CAUTION: The methods have subtly different signatures from the corresponding
 * methods on SparkContext, and as of Spark 1.4.0 the ones on SQLContext
 * are marked "experimental".
 */
object Range {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-Range").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    println("*** dense range with default partitioning")
    val df1 = sqlContext.range(10, 14)
    df1.show()
    println("# Partitions = " + df1.rdd.partitions.length)

    // This overload isn't available on the SQLContext.
    // Probably unintentional ...
    //val df2 = sqlContext.range(10, 14, 2)
    //df2.show()
    //println(df2.rdd.partitions.size)

    println("\n*** stepped range with specified partitioning")
    val df3 = sqlContext.range(10, 14, 2, 2)
    df3.show()
    println("# Partitions = " + df3.rdd.partitions.length)

    // Added in Spark 1.4.1. But so far it only seems to have been
    // added to SQLContext.

    println("\n*** range with just a limit")
    val df4 = sqlContext.range(3)
    df4.show()
    println("# Partitions = " + df4.rdd.partitions.length)
  }
}
