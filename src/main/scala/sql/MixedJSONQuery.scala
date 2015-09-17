package sql

import org.apache.spark.{SparkContext, SparkConf}

object MixedJSONQuery {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("JSON").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val transactions = sqlContext.read.json("src/main/resources/data/mixed.json")
    transactions.printSchema()
    transactions.registerTempTable("transactions")


    val all = sqlContext.sql("SELECT id FROM transactions")
    all.foreach(println)

    val more = sqlContext.sql("SELECT id, since FROM transactions")
    more.foreach(println)

    val deeper = sqlContext.sql("SELECT id, address.zip FROM transactions")
    deeper.foreach(println)

    println("selecting an array valued column")
    val array1 = sqlContext.sql("SELECT id, orders FROM transactions")
    array1.foreach(println)

    println("selecting a specific array element")
    val array2 = sqlContext.sql("SELECT id, orders[0] FROM transactions")
    array2.foreach(println)


  }
}
