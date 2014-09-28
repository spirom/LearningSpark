import org.apache.spark.{SparkContext, SparkConf}

object Ex6_Accumulators {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex6_Accumulators").setMaster("local[4]")
    val sc = new SparkContext(conf)
  }
}
