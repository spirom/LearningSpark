package special

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._

class SpecialPartitioner extends Partitioner {
  def numPartitions = 10

  def getPartition(key: Any) : Int = {
    key match {
      case (x, y:Int, z) => y % numPartitions
      case _ => throw new ClassCastException
    }
  }
}

object CustomPartitioner {
  def analyze[T](r: RDD[T]) : Unit = {
    val partitions = r.glom()
    println(partitions.count() + " parititons")

    // use zipWithIndex() to see the index of each partition
    // we need to loop sequentially so we can see them in order: use collect()
    partitions.zipWithIndex().collect().foreach {
      case (a, i) => {
        println("Partition " + i + " contents (count " + a.count(_ => true) + "):" +
          a.foldLeft("")((e, s) => e + " " + s))
      }
    }
  }

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val triplets =
      for (x <- 1 to 3; y <- 1 to 20; z <- 'a' to 'd')
      yield ((x, y, z), x * y)

    // Spark has the good sense to use the first tuple element
    // for range partitioning, but for this data-set it makes a mess
    val defaultRDD = sc.parallelize(triplets, 10)
    println("with default partitioning")
    analyze(defaultRDD)

    // out custom partitioner uses the second tuple element
    val deliberateRDD = defaultRDD.partitionBy(new SpecialPartitioner())
    println("with deliberate partitioning")
    analyze(deliberateRDD)

  }
}
