import org.apache.spark.{SparkContext, SparkConf}


object Ex4_Partitions {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex4_Partitions").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // look at the distribution of numbers > 5 across partitions
    val numbers =  sc.parallelize(1 to 10, 4)
    numbers.foreachPartition(pi => println(pi.count(_ > 5)))

  }
}
