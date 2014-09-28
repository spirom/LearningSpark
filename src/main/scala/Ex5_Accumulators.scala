import org.apache.spark.{SparkContext, SparkConf}

object Ex5_Accumulators {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex5_Accumulators").setMaster("local[4]")
    val sc = new SparkContext(conf)
  }
}
