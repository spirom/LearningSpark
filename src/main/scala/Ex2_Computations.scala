import org.apache.spark.{SparkContext, SparkConf}

object Ex2_Computations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex2_Computations").setMaster("local[4]")
    val sc = new SparkContext(conf)
  }
}
