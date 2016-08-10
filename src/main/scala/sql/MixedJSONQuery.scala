package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object MixedJSONQuery {
  def main (args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-MixedJSONQuery")
        .master("local[4]")
        .getOrCreate()

    val transactions = spark.read.json("src/main/resources/data/mixed.json")
    transactions.printSchema()
    transactions.createOrReplaceTempView("transactions")


    val all = spark.sql("SELECT id FROM transactions")
    all.foreach(r => println(r))

    val more = spark.sql("SELECT id, since FROM transactions")
    more.foreach(r => println)

    val deeper = spark.sql("SELECT id, address.zip FROM transactions")
    deeper.foreach(r => println(r))

    println("*** selecting an array valued column")
    val array1 = spark.sql("SELECT id, orders FROM transactions")
    array1.foreach(r => println(r))

    println("*** selecting a specific array element")
    val array2 = spark.sql("SELECT id, orders[0] FROM transactions")
    array2.foreach(r => println(r))


  }
}
