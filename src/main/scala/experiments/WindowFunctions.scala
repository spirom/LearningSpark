package experiments

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object WindowFunctions {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-WindowFunctions").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    println("*** stepped range with specified partitioning")
    val df = sqlContext.range(10, 20, 2, 2)
    df.show()
    println("# Partitions = " + df.rdd.partitions.length)

    val ldf = df.select($"id", lag($"id", 2, 0))

    ldf.show()



  }

}
