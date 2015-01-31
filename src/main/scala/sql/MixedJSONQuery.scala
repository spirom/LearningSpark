package sql

import org.apache.spark.{SparkContext, SparkConf}

object MixedJSONQuery {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("JSON").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val transactions = sqlContext.jsonFile("file:///src/main/resources/data/mixed.json")
    transactions.printSchema()
    transactions.registerTempTable("transactions")
    val all = sqlContext.sql("SELECT id FROM transactions")
    all.foreach(println)
  }
}
