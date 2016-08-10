package experiments

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object WindowFunctions {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("Experiments")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    println("*** stepped range with specified partitioning")
    val df = spark.range(10, 20, 2, 2)
    df.show()
    println("# Partitions = " + df.rdd.partitions.length)

    val ldf = df.select($"id", lag($"id", 2, 0))

    ldf.show()



  }

}
