package special

import org.apache.spark.{SparkContext, SparkConf}

object PairRDD {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // TODO: go to town with PairRDD and PairRDDFunctions
  }
}
